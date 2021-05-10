from typing import Any, Dict, Optional, Type

import pandas as pd
import snowflake as pysnowflake
from pydantic import Field, SecretStr

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.snowflake import AuthenticationMethod, SnowflakeDataSource
from toucan_connectors.toucan_connector import ToucanConnector


class DictCursor:
    pass


class SnowflakeoAuth2DataSource(SnowflakeDataSource):
    """Nothing reimplemented from inherited class"""


class SnowflakeoAuth2Connector(ToucanConnector):
    client_id: str = Field(
        ..., Title='Client ID', description='The client id of you Snowflake integration'
    )
    client_secret: SecretStr = Field(
        ..., Title='Client Secret', description='The client secret of your Snowflake integration'
    )
    redirect_uri: str = Field(
        None, Title='Redirect URI', description='The redirect URI called during the oauth2 flow'
    )
    authorization_url: str = Field(
        ..., Title='Authorization URL', description='The authorization URL'
    )
    scope: str = Field(None, Title='Scope', description='The scope the integration')
    token_url: str = Field(
        None, Title='Token URL', description='The URL to refresh the access token'
    )
    auth_flow_id: Optional[str]
    _auth_flow = 'oauth2'
    oauth2_credentials_location = 'connector'
    account: str = Field(...)
    data_source_model: SnowflakeoAuth2DataSource

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['SnowflakeoAuth2Connector']) -> None:
            keys = schema['properties'].keys()
            hidden_keys = ['auth_flow_id', 'oauth2_credentials_location']
            new_keys = [k for k in keys if k not in hidden_keys]
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}

    def __init__(self, *args, **kwargs):
        super().__init__(**{k: v for k, v in kwargs.items() if k != 'secrets_keeper'})
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
        # This needs to be set before we connect
        connection_params = {
            'account': self.account,
            'authenticator': AuthenticationMethod.OAUTH,
            'application': 'ToucanToco',
            'token': self.get_access_token(),
        }
        return pysnowflake.connector.connect(**connection_params)

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        warehouse = data_source.warehouse

        connection = self.connect(
            database=data_source.database,
            warehouse=warehouse,
        )
        cursor = connection.cursor(DictCursor)
        query_res = cursor.execute(data_source.query)
        df = pd.DataFrame(query_res.fetchall())
        connection.close()

        return df
