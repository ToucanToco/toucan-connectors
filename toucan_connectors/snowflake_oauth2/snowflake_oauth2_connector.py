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
from toucan_connectors.toucan_connector import ToucanConnector


class SnowflakeoAuth2DataSource(SnowflakeDataSource):
    """Nothing reimplemented from inherited class"""


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

    def connect(self, database: str, warehouse: str) -> pysnowflake.connector.SnowflakeConnection:
        connection_params = {
            'account': self.account,
            'authenticator': AuthenticationMethod.OAUTH,
            'application': 'ToucanToco',
            'token': self.get_access_token(),
        }
        return pysnowflake.connector.connect(
            **connection_params, database=database, warehouse=warehouse
        )

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        connection = self.connect(
            database=data_source.database,
            warehouse=data_source.warehouse,
        )
        cursor = connection.cursor(DictCursor)
        query_res = cursor.execute(data_source.query)
        df = pd.DataFrame(query_res.fetchall())
        connection.close()

        return df
