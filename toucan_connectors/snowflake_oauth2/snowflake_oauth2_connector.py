import logging
from contextlib import suppress
from timeit import default_timer as timer
from typing import Any, List, Optional

import pandas as pd
import snowflake
from pydantic import Field, SecretStr, create_model
from snowflake.connector import SnowflakeConnection

from toucan_connectors.connection_manager import ConnectionManager
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.snowflake.snowflake_connector import (
    AuthenticationMethod,
    SnowflakeDataSource,
)
from toucan_connectors.snowflake_common import SnowflakeCommon
from toucan_connectors.toucan_connector import Category, DataSlice, ToucanConnector, strlist_to_enum

logger = logging.getLogger(__name__)

connection_manager = None
if not connection_manager:
    connection_manager = ConnectionManager(
        name='snowflake_oauth2', timeout=5, wait=0.2, time_between_clean=3, time_keep_alive=600
    )


class SnowflakeoAuth2DataSource(SnowflakeDataSource):
    @classmethod
    def _get_databases(cls, connector: 'SnowflakeoAuth2Connector'):
        return connector._get_databases()

    @classmethod
    def get_form(cls, connector: 'SnowflakeoAuth2Connector', current_config):
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


class SnowflakeoAuth2Connector(ToucanConnector):
    client_id: str = Field(
        '',
        title='Client ID',
        description='The client id of you Snowflake integration',
        **{'ui.required': True},
    )
    client_secret: SecretStr = Field(
        '',
        title='Client Secret',
        description='The client secret of your Snowflake integration',
        **{'ui.required': True},
    )
    authorization_url: str = Field(None, **{'ui.hidden': True})
    scope: str = Field(
        None, Title='Scope', description='The scope the integration', placeholder='refresh_token'
    )
    token_url: str = Field(None, **{'ui.hidden': True})
    auth_flow_id: str = Field(None, **{'ui.hidden': True})
    _auth_flow = 'oauth2'
    _oauth_trigger = 'connector'
    oauth2_version = Field('1', **{'ui.hidden': True})
    redirect_uri: str = Field(None, **{'ui.hidden': True})
    role: str = Field(
        ...,
        title='Role',
        description='Role to use for queries',
        placeholder='PUBLIC',
    )
    account: str = Field(
        ...,
        title='Account',
        description='The full name of your Snowflake account. '
        'It might require the region and cloud platform where your account is located, '
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info" target="_blank">here</a>.',
    )
    data_source_model: SnowflakeoAuth2DataSource
    default_warehouse: str = Field(
        ..., description='The default warehouse that shall be used for any data source'
    )
    category: Category = Field(Category.SNOWFLAKE, title='category', **{'ui': {'checkbox': False}})

    def __init__(self, **kwargs):
        super().__init__(**{k: v for k, v in kwargs.items() if k != 'secrets_keeper'})
        self.token_url = f'https://{self.account}.snowflakecomputing.com/oauth/token-request'
        self.authorization_url = f'https://{self.account}.snowflakecomputing.com/oauth/authorize'
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=self.authorization_url,
            scope=self.scope,
            token_url=self.token_url,
            redirect_uri=self.redirect_uri,
            config=OAuth2ConnectorConfig(
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
            secrets_keeper=kwargs['secrets_keeper'],
        )

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def _get_connection(self, database: str = None, warehouse: str = None) -> SnowflakeConnection:
        def connect_function() -> SnowflakeConnection:
            logger.info('Connect at Snowflake')
            token_start = timer()
            tokens = self.get_access_token()
            token_end = timer()

            connection_params = {
                'account': self.account,
                'authenticator': AuthenticationMethod.OAUTH,
                'application': 'ToucanToco',
                'token': tokens,
                'role': self.role if self.role else '',
            }
            logger.info(
                f'[benchmark][snowflake] - get_access_token {token_end - token_start} seconds',
                extra={
                    'benchmark': {
                        'operation': 'get_access_token',
                        'execution_time': token_end - token_start,
                        'connector': 'snowflake',
                    }
                },
            )

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

        def alive_function(conn: SnowflakeConnection) -> Any:
            logger.debug('Check Snowflake connection')
            if hasattr(conn, 'is_closed'):
                try:
                    return not conn.is_closed()
                except Exception:
                    raise TypeError('is_closed is not a function')

        def close_function(conn: SnowflakeConnection) -> None:
            logger.debug('Close Snowflake connection')
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

        connection: SnowflakeConnection = connection_manager.get(
            self.identifier,
            connect_method=connect_function,
            alive_method=alive_function,
            close_method=close_function,
            save=True if database and warehouse else False,
        )

        return connection

    def _get_warehouses(self, warehouse_name: Optional[str] = None) -> List[str]:
        with self._get_connection(warehouse=warehouse_name) as connection:
            result = SnowflakeCommon().get_warehouses(connection, warehouse_name)
        return result

    def _get_databases(self, database_name: Optional[str] = None) -> List[str]:
        with self._get_connection(database=database_name) as connection:
            result = SnowflakeCommon().get_databases(connection, database_name)
        return result

    def _retrieve_data(self, data_source: SnowflakeoAuth2DataSource) -> pd.DataFrame:
        with self._get_connection(
            database=data_source.database, warehouse=data_source.warehouse
        ) as connection:
            result = SnowflakeCommon().retrieve_data(connection, data_source)
        return result

    def get_slice(
        self,
        data_source: SnowflakeDataSource,
        permissions: Optional[dict] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> DataSlice:
        with self._get_connection(
            database=data_source.database, warehouse=data_source.warehouse
        ) as connection:
            result = SnowflakeCommon().get_slice(
                connection,
                data_source,
                offset=offset,
                limit=limit,
            )
        return result

    @staticmethod
    def get_connection_manager():
        return connection_manager
