import logging
from contextlib import contextmanager, suppress
from datetime import datetime
from enum import Enum
from typing import Any, ContextManager, Generator, Literal, Type, overload

import jwt
import pandas as pd
import requests
import snowflake
from jinja2 import Template
from pydantic import Field, SecretStr, create_model
from snowflake import connector as sf_connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor as SfDictCursor

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.pagination import build_pagination_info
from toucan_connectors.snowflake_common import (
    build_database_model_extraction_query,
    type_code_mapping,
)
from toucan_connectors.sql_query_helper import SqlQueryHelper
from toucan_connectors.toucan_connector import (
    Category,
    DataSlice,
    DiscoverableConnector,
    TableInfo,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

logger = logging.getLogger(__name__)

# TODO: Once we remove SnowflakeCommon, directly assign the query block here
_DB_MODEL_EXTRACTION_QUERY = build_database_model_extraction_query()
# TODO: Once we remove SnowflakeCommon, declare the mapping here
_TYPE_CODE_MAPPING = type_code_mapping

_UI_HIDDEN: dict[str, Any] = {'ui.hidden': True}


class SnowflakeDataSource(ToucanDataSource['SnowflakeConnector']):
    database: str = Field(..., description='The name of the database you want to query')
    warehouse: str | None = Field(None, description='The name of the warehouse you want to query')

    query: str = Field(
        ..., description='You can write your SQL query here', min_length=1, widget='sql'
    )

    # Pydantic sees **_UI_HIDDEN as the third argument (the default factory) and raises an error
    query_object: dict | None = Field(  # type: ignore[pydantic-field]
        None,
        description='An object describing a simple select query'
        'For example '
        '{"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]}'
        'This field is used internally',
        **_UI_HIDDEN,
    )
    language: str = Field('sql', **_UI_HIDDEN)  # type: ignore[pydantic-field]

    @classmethod
    def _get_databases(cls, connector: 'SnowflakeConnector'):
        return connector._get_databases()

    @classmethod
    def get_form(cls, connector: 'SnowflakeConnector', current_config):
        constraints: dict[str, Any] = {}

        with suppress(Exception):
            databases = connector._get_databases()
            warehouses = connector._get_warehouses()
            # Restrict some fields to lists of existing counterparts
            constraints['database'] = strlist_to_enum('database', databases)
            constraints['warehouse'] = strlist_to_enum('warehouse', warehouses)

        res = create_model('FormSchema', __base__=cls, **constraints).schema()
        res['properties']['warehouse']['default'] = connector.default_warehouse
        return res


class AuthenticationMethod(str, Enum):
    PLAIN: str = 'Snowflake (ID + Password)'
    OAUTH: str = 'oAuth'


class AuthenticationMethodValue(str, Enum):
    PLAIN: str = 'snowflake'
    OAUTH: str = 'oauth'


@contextmanager
def _snowflake_connection(
    **args: str | int | None,
) -> Generator[SnowflakeConnection, None, None]:
    """Returns a Snowflake connection and automatically closes it."""
    sf_connector.paramstyle = 'qmark'
    conn = SnowflakeConnection(**args)  # type:ignore[arg-type]
    try:
        yield conn
    finally:
        # Entire method is already wrapped in a try/except block
        conn.close()


class SnowflakeConnector(ToucanConnector[SnowflakeDataSource], DiscoverableConnector):
    """
    Import data from Snowflake data warehouse.
    """

    _sso_credentials_access: bool = True

    sso_credentials_keeper: Any = None
    user_tokens_keeper: Any = None

    data_source_model: SnowflakeDataSource

    account: str = Field(
        ...,
        description='The full name of your Snowflake account. '
        'It might require the region and cloud platform where your account is located, '
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info" target="_blank">here</a>.',
        placeholder='your_account_name.region_id.cloud_platform',
    )

    authentication_method: AuthenticationMethod = Field(
        AuthenticationMethod.PLAIN,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your snowflake data source',
        ui={'checkbox': False},
    )

    user: str = Field(..., description='Your login username')
    password: SecretStr | None = Field(None, description='Your login password')
    token_endpoint: str | None = Field(None, description='The token endpoint')
    token_endpoint_content_type: str = Field(
        'application/json',
        description='The content type to use when requesting the token endpoint',
    )

    role: str | None = Field(
        None,
        description='The user role that you want to connect with. '
        'See more details <a href="https://docs.snowflake.com/en/user-guide/admin-user-management.html#user-roles" target="_blank">here</a>.',
    )

    default_warehouse: str | None = Field(
        None, description='The default warehouse that shall be used for any data source'
    )
    category: Category = Field(Category.SNOWFLAKE, title='category', ui={'checkbox': False})

    class Config:
        @staticmethod
        def schema_extra(schema: dict[str, Any], model: Type['SnowflakeConnector']) -> None:
            ordered_keys = [
                'type',
                'name',
                'account',
                'authentication_method',
                'user',
                'password',
                'token_endpoint',
                'token_endpoint_content_type',
                'role',
                'default_warehouse',
                'retry_policy',
                'secrets_storage_version',
                'sso_credentials_keeper',
                'user_tokens_keeper',
            ]
            schema['properties'] = {k: schema['properties'][k] for k in ordered_keys}

    @property
    def access_token(self) -> str | None:
        return self.user_tokens_keeper and self.user_tokens_keeper.access_token.get_secret_value()

    @property
    def refresh_token(self) -> str | None:
        return self.user_tokens_keeper and self.user_tokens_keeper.refresh_token.get_secret_value()

    @property
    def client_id(self) -> str | None:
        return self.sso_credentials_keeper and self.sso_credentials_keeper.client_id

    @property
    def client_secret(self) -> str | None:
        return (
            self.sso_credentials_keeper
            and self.sso_credentials_keeper.client_secret.get_secret_value()
        )

    @staticmethod
    def _get_status_details(index: int, status: bool | None) -> list[tuple[str, bool | None]]:
        checks = ['Connection to Snowflake', 'Default warehouse exists']
        ok_checks: list[tuple[str, bool | None]] = [
            (check, True) for i, check in enumerate(checks) if i < index
        ]
        new_check = (checks[index], status)
        not_validated_checks: list[tuple[str, bool | None]] = [
            (check, None) for i, check in enumerate(checks) if i > index
        ]
        return ok_checks + [new_check] + not_validated_checks

    def get_status(self) -> ConnectorStatus:
        try:
            res = self._get_warehouses(self.default_warehouse)
            if len(res) == 0:
                return ConnectorStatus(
                    status=False,
                    details=self._get_status_details(1, False),
                    error=f"The warehouse '{self.default_warehouse}' does not exist.",
                )
        except snowflake.connector.errors.OperationalError:
            # Raised when the provided account does not exists or when the
            # provided User does not have access to the provided account
            return ConnectorStatus(
                status=False,
                details=self._get_status_details(0, False),
                error=f"Connection failed for the account '{self.account}', please check the Account field",
            )
        except snowflake.connector.errors.ForbiddenError:
            return ConnectorStatus(
                status=False,
                details=self._get_status_details(0, False),
                error=f"Access forbidden, please check that you have access to the '{self.account}' account or try again later.",
            )
        except snowflake.connector.errors.ProgrammingError as e:
            return ConnectorStatus(
                status=False, details=self._get_status_details(0, False), error=str(e)
            )
        except snowflake.connector.errors.DatabaseError:
            # Raised when the provided User/Password aren't correct
            return ConnectorStatus(
                status=False,
                details=self._get_status_details(0, False),
                error=f"Connection failed for the user '{self.user}', please check your credentials",
            )

        return ConnectorStatus(status=True, details=self._get_status_details(1, True), error=None)

    def get_connection_params(self) -> dict[str, str | int]:
        params: dict[str, str | int] = {
            'user': Template(self.user).render(),
            'account': self.account,
            'authenticator': self.authentication_method,
            # hard Snowflake params
            'application': 'ToucanToco',
            'client_session_keep_alive_heartbeat_frequency': 59,
            'client_prefetch_threads': 5,
            'session_id': self.identifier,
        }

        if params['authenticator'] == AuthenticationMethod.PLAIN and self.password:
            params['authenticator'] = AuthenticationMethodValue.PLAIN
            params['password'] = self.password.get_secret_value()

        if self.authentication_method == AuthenticationMethod.OAUTH:
            if self.access_token is not None:
                params['token'] = self.access_token
            params['authenticator'] = AuthenticationMethodValue.OAUTH

        if self.role:
            params['role'] = self.role

        return params

    def _refresh_oauth_token(self):
        """Regenerates an oauth token.

        Only does so if configuration was provided and if the given token has expired.
        """
        if self.token_endpoint and self.refresh_token:
            try:
                # Here, we only want to access the expiration date, we don't care if the token is
                # not valid. The options should be enough according to docs
                access_token = jwt.decode(
                    self.access_token,
                    options={
                        'verify_signature': False,
                        'verify_exp': False,
                        'verify_nbf': False,
                        'verify_iat': False,
                        'verify_aud': False,
                        'verify_iss': False,
                    },
                )
                is_expired = datetime.fromtimestamp(access_token['exp']) < datetime.now()
            except Exception:
                is_expired = True

            if is_expired:
                res = requests.post(
                    self.token_endpoint,
                    data={
                        'grant_type': 'refresh_token',
                        'client_id': self.client_id,
                        'client_secret': self.client_secret,
                        'refresh_token': self.refresh_token,
                    },
                    headers={'Content-Type': self.token_endpoint_content_type},
                )
                res.raise_for_status()

                self.user_tokens_keeper.update_tokens(
                    access_token=res.json().get('access_token'),
                    refresh_token=res.json().get('refresh_token'),
                )

    def _get_connection(
        self, database: str | None = None, warehouse: str | None = None
    ) -> ContextManager[SnowflakeConnection]:
        if self.authentication_method == AuthenticationMethod.OAUTH:
            logger.info('Refreshing OAuth token...')
            self._refresh_oauth_token()
            logger.info('Done refreshing OAuth token')

        return _snowflake_connection(
            **self.get_connection_params(), database=database, warehouse=warehouse
        )

    def _set_warehouse(self, data_source: SnowflakeDataSource):
        return (
            data_source.copy(update={'warehouse': self.default_warehouse})
            if not data_source.warehouse
            else data_source
        )

    @overload
    def _execute_query(
        self,
        query: str,
        parameters: dict | list[str] | None = None,
        *,
        warehouse: str | None = None,
        database: str | None = None,
        as_df: Literal[True] = ...,
        snowflake_connection: SnowflakeConnection | None = None,
    ) -> pd.DataFrame:
        ...  # pragma: no cover

    @overload
    def _execute_query(
        self,
        query: str,
        parameters: dict | list[str] | None = None,
        *,
        warehouse: str | None = None,
        database: str | None = None,
        as_df: Literal[False],
        snowflake_connection: SnowflakeConnection | None = None,
    ) -> list[dict]:
        ...  # pragma: no cover

    def _execute_query(
        self,
        query: str,
        parameters: dict | list[str] | None = None,
        *,
        warehouse: str | None = None,
        database: str | None = None,
        as_df: bool = True,
        snowflake_connection: SnowflakeConnection | None = None,
    ) -> pd.DataFrame | list[dict]:
        def _execute(conn: SnowflakeConnection) -> pd.DataFrame | list[dict]:
            curs = conn.cursor(SfDictCursor)
            query_result = curs.execute(query, parameters)
            assert query_result is not None
            # snowflake typing is incomplete for DictCursor
            results: list[dict] = query_result.fetchall()  # type:ignore[assignment]
            return pd.DataFrame(results) if as_df else results

        if snowflake_connection is not None:
            return _execute(snowflake_connection)
        with self._get_connection(database=database, warehouse=warehouse) as conn:
            return _execute(conn)

    def _describe_query(self, query: str) -> dict[str, str]:
        with self._get_connection() as conn:
            curs = conn.cursor(SfDictCursor)
            return {r.name: _TYPE_CODE_MAPPING[r.type_code] for r in curs.describe(query)}

    def _get_warehouses(self, warehouse_name: str | None = None) -> list[str]:
        query = 'SHOW WAREHOUSES'
        if warehouse_name:
            query += f" LIKE '{warehouse_name}'"

        result = self._execute_query(query, warehouse=warehouse_name)
        return result['name'].to_list() if 'name' in result.columns else []

    def _get_databases(self, database_name: str | None = None) -> list[str]:
        query = 'SHOW DATABASES'
        if database_name:
            query += f" LIKE '{database_name}'"

        result = self._execute_query(query, database=database_name)
        return result['name'].to_list() if 'name' in result.columns else []

    def _fetch_data(
        self,
        data_source: SnowflakeDataSource,
        offset: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        data_source = self._set_warehouse(data_source)

        prepared_query, prepared_params = SqlQueryHelper.prepare_limit_query(
            data_source.query, data_source.parameters, offset=offset, limit=limit
        )
        return self._execute_query(
            prepared_query,
            prepared_params,
            database=data_source.database,
            warehouse=data_source.warehouse,
        )

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        return self._fetch_data(data_source)

    def get_slice(
        self,
        data_source: SnowflakeDataSource,
        permissions: dict | None = None,
        offset: int = 0,
        limit: int | None = None,
        get_row_count: bool | None = False,
    ) -> DataSlice:
        # We assume permissions have been applied earlier
        df = self._fetch_data(data_source, offset=offset, limit=limit)
        return DataSlice(
            df=df,
            pagination_info=build_pagination_info(
                offset=0, limit=limit, total_rows=None, retrieved_rows=len(df)
            ),
        )

    def describe(self, data_source: SnowflakeDataSource) -> dict[str, str]:
        return self._describe_query(data_source.query)

    def _get_unique_datasource_identifier(self, data_source: SnowflakeDataSource) -> dict[str, Any]:
        prepared_query, prepared_query_parameters = SqlQueryHelper.prepare_query(
            data_source.query, data_source.parameters
        )
        return {
            'warehouse': data_source.warehouse,
            'database': data_source.database,
            'query': prepared_query,
            'parameters': prepared_query_parameters,
        }

    def get_model(self, db_name: str | None = None) -> list[TableInfo]:
        if db_name is None:
            databases = self._get_databases()
        else:
            databases = [db_name]

        # We need to execute the query for every database in case None is specified
        values: list[tuple] = []
        for db in databases:
            with self._get_connection(database=db) as conn:
                values.extend(
                    [
                        tuple(elem.values())
                        for elem in self._execute_query(
                            _DB_MODEL_EXTRACTION_QUERY,
                            database=db_name,
                            as_df=False,
                            snowflake_connection=conn,
                        )
                    ]
                )

        return self.format_db_model(values)
