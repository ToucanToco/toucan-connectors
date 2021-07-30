import pandas as pd
from pydantic import Field
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class SpotifyDataSource(ToucanDataSource):
    # fields
    query: str = Field(
        'daft punk',
        placeholder='spotify-query',
        description='The search element we want to send to spotify and get results like the artist name',
    )
    type_data: str = Field(
        'track',
        placeholder='track / playlist /...',
        description="The type of data, you're fetching from spotify",
    )

    limit: int = Field(
        10,
        placeholder=10,
        description='The number of elements we want',
    )
    offset: int = Field(
        5,
        placeholder=5,
        description='The offset number of elements we want',
    )


class SpotifyConnector(ToucanConnector):
    # constants for coverage
    ui_hidden = {'ui.hidden': True}
    ui_required = {'ui.required': True}
    name: str = Field(
        'Toucan Toco Spotify',
        title='Client ID',
        description='The name of the connector',
        **ui_required,
    )
    client_id: str = Field(
        '',
        title='Client ID',
        placeholder='9dc5eae093274640ba537d928be0db63',
        description='The client id of your Spotify application',
        **ui_required,
    )
    client_secret: str = Field(
        '',
        title='Client Secret',
        placeholder='8c3d809636d54b8db7f83e178accc0a7',
        description='The client secret of your Spotify application',
        **ui_required,
    )
    scope: str = Field(
        'user-read-private',
        Title='Scope',
        description='The scope the integration',
        placeholder='user-read-private',
    )
    authorization_url: str = Field(None, **ui_hidden)
    token_url: str = Field(None, **ui_hidden)
    auth_flow_id: str = Field(None, **ui_hidden)
    _auth_flow = 'oauth2'
    _oauth_trigger = 'connector'
    oauth2_version = Field('1', **ui_hidden)
    redirect_uri: str = Field(None, **ui_hidden)

    data_source_model: SpotifyDataSource

    def __init__(self, **kwargs):
        super().__init__(**{k: v for k, v in kwargs.items() if k != 'secrets_keeper'})
        self.token_url = 'https://accounts.spotify.com/api/token'
        self.authorization_url = 'https://accounts.spotify.com/authorize'
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            secrets_keeper=kwargs['secrets_keeper'],
            redirect_uri=self.redirect_uri,
            authorization_url=self.authorization_url,
            scope=self.scope,
            config=OAuth2ConnectorConfig(
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
            token_url=self.token_url,
            auth_flow_id=self.auth_flow_id,
        )

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def retrieve_tokens(self, authorization_response: str):
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    # To maintain a connection
    def connect(self) -> SpotifyOAuth:
        return SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope=self.scope,
        )

    def _retrieve_data(self, data_source: SpotifyDataSource) -> pd.DataFrame:
        return pd.DataFrame(
            Spotify(auth_manager=self.connect()).search(
                q=data_source.query,
                type=data_source.type_data,
                limit=data_source.limit,
                offset=data_source.offset,
            )
        )
