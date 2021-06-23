import logging
from timeit import default_timer as timer
from typing import List

import pandas as pd
import snowflake as pysnowflake
from pydantic import Field, SecretStr
from snowflake.connector import DictCursor

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.snowflake.snowflake_connector import (
    AuthenticationMethod,
    SnowflakeDataSource,
)
from toucan_connectors.toucan_connector import Category, ToucanConnector

logger = logging.getLogger(__name__)


class SnowflakeoAuth2DataSource(SnowflakeDataSource):
    """ """


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

    def _get_warehouses(self) -> List[str]:
        with self.connect() as connection:
            return [
                warehouse['name']
                for warehouse in connection.cursor(DictCursor).execute('SHOW WAREHOUSES').fetchall()
                if 'name' in warehouse
            ]

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def connect(self, database=None, warehouse=None) -> pysnowflake.connector.SnowflakeConnection:
        token_start = timer()
        connection_params = {
            'account': self.account,
            'authenticator': AuthenticationMethod.OAUTH,
            'application': 'ToucanToco',
            'token': self.get_access_token(),
            'role': self.role if self.role else '',
        }
        token_end = timer()
        logger.info(
            f'[benchmark] - get_access_token {token_end - token_start} seconds',
            extra={
                'benchmark': {
                    'operation': 'get_access_token',
                    'execution_time': token_end - token_start,
                }
            },
        )
        return pysnowflake.connector.connect(
            **connection_params, database=database, warehouse=warehouse
        )

    def _retrieve_data(self, data_source: SnowflakeoAuth2DataSource) -> pd.DataFrame:
        connect_start = timer()
        connection = self.connect(
            database=data_source.database,
            warehouse=data_source.warehouse,
        )
        connect_end = timer()
        logger.info(
            f'[benchmark] - connect {connect_end - connect_start} seconds',
            extra={
                'benchmark': {
                    'operation': 'connect',
                    'execution_time': connect_end - connect_start,
                }
            },
        )
        execution_start = timer()
        cursor = connection.cursor(DictCursor)
        query_res = cursor.execute(data_source.query)
        df = pd.DataFrame(query_res.fetchall())
        connection.close()
        execution_end = timer()
        logger.info(
            f'[benchmark] - execute {execution_end - execution_start} seconds',
            extra={
                'benchmark': {
                    'operation': 'execute',
                    'execution_time': execution_end - execution_start,
                }
            },
        )
        return df
