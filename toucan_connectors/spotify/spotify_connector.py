from typing import Any, Dict, Type

import pandas as pd
from pydantic import Field
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

spotify_connection = None


# To maintain a connection
def get_spotipy_connect(client_id, client_secret):
    global spotify_connection
    # The spotify object for connection
    if spotify_connection is None:
        spotify_connection = Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
        )
    return spotify_connection


class SpotifyDataSource(ToucanDataSource):
    # fields
    name: str = Field(
        'spotify',
        description='Spotify Name',
    )
    domain: str = Field(
        'https://spotify.com',
        description='Spotify host',
    )
    query: str = Field(
        'daft punk',
        description='The search element we want to send to spotify and get results like the artist name',
    )
    limit: int = Field(
        10,
        description='The number of elements we want',
    )

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['SpotifyDataSource']) -> None:
            keys = schema['properties'].keys()
            last_keys = ['name', 'domain', 'query', 'limit']
            new_keys = [k for k in keys if k not in last_keys] + last_keys
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}


class SpotifyConnector(ToucanConnector):
    data_source_model: SpotifyDataSource

    # credentials
    client_id: str = Field(
        '',
        description='The client id of your application'
    )
    client_secret: str = Field(
        '',
        description='The client secret of your application'
    )

    def get_df(self, data_source: SpotifyDataSource, permissions=None):
        return self._retrieve_data(data_source)

    def _retrieve_data(self, data_source: SpotifyDataSource) -> pd.DataFrame:
        # We fetch results depending on the type
        results = get_spotipy_connect(
            client_id=self.client_id,
            client_secret=self.client_secret
        ).search(
            q=data_source.query,
            limit=data_source.limit
        )

        return pd.DataFrame(results)
