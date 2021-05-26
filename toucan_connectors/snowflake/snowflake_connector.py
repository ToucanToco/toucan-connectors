from contextlib import suppress
from datetime import datetime
from enum import Enum
from os import path
from typing import Any, Dict, List, Optional, Type

import jwt
import pandas as pd
import requests
import snowflake.connector
from jinja2 import Template
from pydantic import Field, SecretStr, constr, create_model
from snowflake.connector import DictCursor

from toucan_connectors.common import (
    ConnectorStatus,
    convert_to_printf_templating_style,
    convert_to_qmark_paramstyle,
)
from toucan_connectors.toucan_connector import (
    Category,
    DataSlice,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)


class Path(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not path.exists(v):
            raise ValueError(f'path does not exists: {v}')
        return v


class SnowflakeConnectorException(Exception):
    """Raised when something wrong happened in a snowflake context"""


class SnowflakeConnectorWarehouseDoesNotExists(Exception):
    """Raised when the specified default warehouse does not exists"""


class SnowflakeDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    warehouse: str = Field(None, description='The name of the warehouse you want to query')

    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )

    @classmethod
    def _get_databases(cls, connector: 'SnowflakeConnector'):
        # FIXME: Maybe use a generator instead of a list here?
        with connector.connect() as connection:
            return [
                db['name']
                # Fetch rows as dicts with column names as keys
                for db in connection.cursor(DictCursor).execute('SHOW DATABASES').fetchall()
                if 'name' in db
            ]

    @classmethod
    def get_form(cls, connector: 'SnowflakeConnector', current_config):
        constraints = {}

        with suppress(Exception):
            databases = cls._get_databases(connector)
            warehouses = connector._get_warehouses()
            # Restrict some fields to lists of existing counterparts
            constraints['database'] = strlist_to_enum('database', databases)
            constraints['warehouse'] = strlist_to_enum('warehouse', warehouses)

        res = create_model('FormSchema', **constraints, __base__=cls).schema()
        res['properties']['warehouse']['default'] = connector.default_warehouse
        return res


class AuthenticationMethod(str, Enum):
    PLAIN: str = 'Snowflake (ID + Password)'
    OAUTH: str = 'oAuth'


class AuthenticationMethodValue(str, Enum):
    PLAIN: str = 'snowflake'
    OAUTH: str = 'oauth'


class SnowflakeConnector(ToucanConnector):
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
        AuthenticationMethod.PLAIN.value,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your snowflake data source',
        **{'ui': {'checkbox': False}},
    )

    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    token_endpoint: Optional[str] = Field(None, description='The token endpoint')
    token_endpoint_content_type: str = Field(
        'application/json',
        description='The content type to use when requesting the token endpoint',
    )

    role: str = Field(
        None,
        description='The user role that you want to connect with. '
        'See more details <a href="https://docs.snowflake.com/en/user-guide/admin-user-management.html#user-roles" target="_blank">here</a>.',
    )

    default_warehouse: str = Field(
        None, description='The default warehouse that shall be used for any data source'
    )
    category: Category = Field(Category.SNOWFLAKE, title='category', **{'ui': {'checkbox': False}})

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['SnowflakeConnector']) -> None:
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

    @staticmethod
    def _get_status_details(index: int, status: Optional[bool]):
        checks = ['Connection to Snowflake', 'Default warehouse exists']
        ok_checks = [(check, True) for i, check in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(check, None) for i, check in enumerate(checks) if i > index]
        return ok_checks + [new_check] + not_validated_checks

    @property
    def access_token(self) -> Optional[str]:
        return self.user_tokens_keeper and self.user_tokens_keeper.access_token.get_secret_value()

    @property
    def refresh_token(self) -> Optional[str]:
        return self.user_tokens_keeper and self.user_tokens_keeper.refresh_token.get_secret_value()

    @property
    def client_id(self) -> Optional[str]:
        return self.sso_credentials_keeper and self.sso_credentials_keeper.client_id

    @property
    def client_secret(self) -> Optional[str]:
        return (
            self.sso_credentials_keeper
            and self.sso_credentials_keeper.client_secret.get_secret_value()
        )

    def get_status(self) -> ConnectorStatus:
        try:
            with self.connect(login_timeout=5) as connection:
                cursor = connection.cursor()
                self._execute_query(cursor, 'SHOW WAREHOUSES', {})

                # Check if the default specified warehouse exists
                res = self._execute_query(
                    cursor, f"SHOW WAREHOUSES LIKE '{self.default_warehouse}'", {}
                )
                if res.empty:
                    raise SnowflakeConnectorWarehouseDoesNotExists(
                        f"The warehouse '{self.default_warehouse}' does not exist."
                    )

        except SnowflakeConnectorWarehouseDoesNotExists as e:
            return ConnectorStatus(
                status=False, details=self._get_status_details(1, False), error=str(e)
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

    def get_connection_params(self):
        res = {
            'user': Template(self.user).render(),
            'account': self.account,
            'authenticator': self.authentication_method,
            'application': 'ToucanToco',
        }

        if not self.authentication_method:
            # Default to User/Password authentication method if the parameter
            # was not set when the connector was created
            res['authenticator'] = AuthenticationMethodValue.PLAIN

        if res['authenticator'] == AuthenticationMethod.PLAIN and self.password:
            res['authenticator'] = AuthenticationMethodValue.PLAIN
            res['password'] = self.password.get_secret_value()

        if self.authentication_method == AuthenticationMethod.OAUTH:
            if self.access_token is not None:
                res['token'] = self.access_token
            res['authenticator'] = AuthenticationMethodValue.OAUTH

        if self.role != '':
            res['role'] = self.role

        return res

    def _refresh_oauth_token(self):
        """Regenerates an oauth token if configuration was provided and if the given token has expired."""
        if self.token_endpoint and self.refresh_token:
            access_token = jwt.decode(
                self.access_token,
                verify=False,
                options={'verify_signature': False},
            )
            if datetime.fromtimestamp(access_token['exp']) < datetime.now():
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

    def connect(self, **kwargs) -> snowflake.connector.SnowflakeConnection:
        # This needs to be set before we connect
        snowflake.connector.paramstyle = 'qmark'
        if self.authentication_method == AuthenticationMethod.OAUTH:
            self._refresh_oauth_token()
        connection_params = self.get_connection_params()

        return snowflake.connector.connect(**connection_params, **kwargs)

    def _get_warehouses(self) -> List[str]:
        with self.connect() as connection:
            return [
                warehouse['name']
                for warehouse in connection.cursor(DictCursor).execute('SHOW WAREHOUSES').fetchall()
                if 'name' in warehouse
            ]

    def _execute_query(
        self, cursor, query: str, query_parameters: Dict, max_rows: Optional[int] = None
    ):
        """Executes `query` against Snowflake's client and retrieves the
        results.

        Args:
            cursor (SnowflakeCursor): A snowflake database cursor
            query (str): The query that will be executed against a database
            query_parameters (Dict): The eventual parameters that will be applied to `query`

        Returns: A pandas DataFrame
        """
        # Prevent error with dict and array values in the parameter object
        query = convert_to_printf_templating_style(query)
        converted_query, ordered_values = convert_to_qmark_paramstyle(query, query_parameters)

        query_res = cursor.execute(converted_query, ordered_values)
        # https://docs.snowflake.com/en/user-guide/python-connector-api.html#fetch_pandas_all
        # `fetch_pandas_all` will only work with `SELECT` queries, if the
        # query does not contains 'SELECT' then we're defaulting to the usual
        # `fetchall`.
        if 'SELECT' in query.upper():
            if max_rows is None:
                values = query_res.fetch_pandas_all()
            else:
                values = pd.DataFrame.from_dict(query_res.fetchmany(max_rows))
        else:
            if max_rows is None:
                values = pd.DataFrame.from_dict(query_res.fetchall())
            else:
                values = pd.DataFrame.from_dict(query_res.fetchmany(max_rows))

        return values

    def _fetch_data(
        self, data_source: SnowflakeDataSource, max_rows: Optional[int]
    ) -> pd.DataFrame:
        warehouse = data_source.warehouse
        # Default to default warehouse if not specified in `data_source`
        if self.default_warehouse and not warehouse:
            warehouse = self.default_warehouse

        with self.connect(
            database=Template(data_source.database).render(),
            warehouse=Template(warehouse).render(),
        ) as connection:
            cursor = connection.cursor(DictCursor)
            df = self._execute_query(cursor, data_source.query, data_source.parameters, max_rows)

        return df

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        return self._fetch_data(data_source, None)

    def get_slice(
        self,
        data_source: SnowflakeDataSource,
        permissions: Optional[dict] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> DataSlice:

        rows_to_fetch = offset + limit
        df = self._fetch_data(data_source, rows_to_fetch)

        return DataSlice(df[offset:], len(df[offset:]))
