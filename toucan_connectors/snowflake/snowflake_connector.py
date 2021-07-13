import logging
from contextlib import suppress
from datetime import datetime
from enum import Enum
from os import path
from timeit import default_timer as timer
from typing import Any, Dict, List, Optional, Type

import jwt
import pandas as pd
import requests
import snowflake
from jinja2 import Template
from pydantic import Field, SecretStr, create_model
from snowflake.connector import SnowflakeConnection

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.connection_manager import ConnectionManager
from toucan_connectors.snowflake_common import (
    SfDataSource,
    SnowflakeCommon,
    SnowflakeConnectorWarehouseDoesNotExists,
)
from toucan_connectors.toucan_connector import Category, DataSlice, ToucanConnector, strlist_to_enum

logger = logging.getLogger(__name__)

snowflake_connection_manager = None
if not snowflake_connection_manager:
    snowflake_connection_manager = ConnectionManager(
        name='snowflake', timeout=10, wait=0.2, time_between_clean=10, time_keep_alive=600
    )


class Path(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate  # pragma: no cover

    @classmethod
    def validate(cls, v):
        if not path.exists(v):  # pragma: no cover
            raise ValueError(f'path does not exists: {v}')  # pragma: no cover
        return v  # pragma: no cover


class SnowflakeDataSource(SfDataSource):
    @classmethod
    def _get_databases(cls, connector: 'SnowflakeConnector'):
        return connector._get_databases()

    @classmethod
    def get_form(cls, connector: 'SnowflakeConnector', current_config):
        constraints = {}

        with suppress(Exception):
            databases = connector._get_databases()
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
            res = self._get_warehouses(self.default_warehouse)
            if len(res) == 0:
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
        params = {
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

        if self.role != '':
            params['role'] = self.role

        return params

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

    def _get_connection(self, database: str = None, warehouse: str = None) -> SnowflakeConnection:
        def connect_function() -> SnowflakeConnection:
            logger.info('Connect at Snowflake')
            snowflake.connector.paramstyle = 'qmark'
            if self.authentication_method == AuthenticationMethod.OAUTH:
                token_start = timer()
                self._refresh_oauth_token()
                token_end = timer()
                logger.info(
                    f'[benchmark] - _refresh_oauth_token {token_end - token_start} seconds',
                    extra={
                        'benchmark': {
                            'operation': '_refresh_oauth_token',
                            'execution_time': token_end - token_start,
                            'connector': 'snowflake',
                        }
                    },
                )

            connection_params = self.get_connection_params()
            logger.info(
                f'Connect at Snowflake with {connection_params}, database {database} and warehouse {warehouse}'
            )
            connect_start = timer()
            connection = snowflake.connector.connect(
                **connection_params, database=database, warehouse=warehouse
            )
            connect_end = timer()
            logger.info(
                f'[benchmark][snowflake] - connect {connect_end - connect_start} seconds',
                extra={
                    'benchmark': {
                        'operation': 'connect',
                        'execution_time': connect_end - connect_start,
                        'connector': 'snowflake',
                    }
                },
            )
            return connection

        def alive_function(conn):
            logger.debug('Check Snowflake connection alive')
            if hasattr(conn, 'is_closed'):
                try:
                    return not conn.is_closed()
                except Exception:
                    raise TypeError('is_closed is not a function')

        def close_function(conn):
            logger.info('Close Snowflake connection')
            if hasattr(conn, 'close'):
                try:
                    close_start = timer()
                    r = conn.close()
                    close_end = timer()
                    logger.info(
                        f'[benchmark][snowflake] - close {close_end - close_start} seconds',
                        extra={
                            'benchmark': {
                                'operation': 'close',
                                'execution_time': close_end - close_start,
                                'connector': 'snowflake',
                            }
                        },
                    )
                    return r
                except Exception:
                    raise TypeError('close is not a function')

        connection = snowflake_connection_manager.get(
            identifier=self.identifier,
            connect_method=connect_function,
            alive_method=alive_function,
            close_method=close_function,
            save=True if self.identifier is not None and database and warehouse else False,
        )

        return connection

    def _set_warehouse(self, data_source: SnowflakeDataSource):
        warehouse = data_source.warehouse
        if self.default_warehouse and not warehouse:
            data_source.warehouse = self.default_warehouse
        return data_source

    def _get_warehouses(self, warehouse_name: Optional[str] = None) -> List[str]:
        with self._get_connection(warehouse=warehouse_name) as connection:
            result = SnowflakeCommon().get_warehouses(connection, warehouse_name)
        return result

    def _get_databases(self, database_name: Optional[str] = None) -> List[str]:
        with self._get_connection(database=database_name) as connection:
            result = SnowflakeCommon().get_databases(connection, database_name)
        return result

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        with self._get_connection(data_source.database, data_source.warehouse) as connection:
            result = SnowflakeCommon().retrieve_data(connection, data_source)
        return result

    def _fetch_data(
        self,
        data_source: SnowflakeDataSource,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        get_row_count: bool = False,
    ) -> pd.DataFrame:
        warehouse = data_source.warehouse
        # Default to default warehouse if not specified in `data_source`
        if self.default_warehouse and not warehouse:
            data_source.warehouse = self.default_warehouse
        with self._get_connection(data_source.database, data_source.warehouse) as connection:
            result = SnowflakeCommon().fetch_data(
                connection, data_source, offset, limit, get_row_count
            )
        return result

    def get_slice(
        self,
        data_source: SnowflakeDataSource,
        permissions: Optional[dict] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> DataSlice:
        with self._get_connection(data_source.database, data_source.warehouse) as connection:
            result = SnowflakeCommon().get_slice(connection, data_source, offset, limit)
        return result

    @staticmethod
    def get_snowflake_connection_manager():
        return snowflake_connection_manager
