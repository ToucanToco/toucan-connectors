import pytest

from toucan_connectors.spotify.spotify_connector import SpotifyConnector, SpotifyDataSource


@pytest.fixture
def spotify_oauth2_connector(secrets_keeper):
    return SpotifyConnector(
        name='toucan-toco',
        client_id='9dc5eae093274640ba537d928be0db63',
        client_secret='8c3d809636d54b8db7f83e178accc0a7',
        scope='user-top-read',
        token_url='https://accounts.spotify.com/api/token',
        redirect_uri='http://127.0.0.1:5000/api_callback/',
        secrets_keeper=secrets_keeper,
    )


@pytest.fixture
def spotify_oauth2_data_source():
    return SpotifyDataSource(
        name='spotify',
        domain='https://spotify.com',
        query='daft punk',
        type_data='track',
        limit=10,
        offset=5,
    )
