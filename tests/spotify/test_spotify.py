from spotipy.oauth2 import SpotifyOAuth

from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector
from toucan_connectors.spotify.spotify_connector import SpotifyConnector


def test_build_authorization_url(mocker, spotify_oauth2_connector):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    spotify_oauth2_connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    spotify_oauth2_connector.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_tokens(mocker, spotify_oauth2_connector):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    spotify_oauth2_connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    spotify_oauth2_connector.retrieve_tokens('bla')
    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_get_access_token(mocker, spotify_oauth2_connector):
    """
    Check that get_access correctly calls oAuth2Connector's get_access_token
    """
    mocked_oauth2_get_access_token = mocker.patch(
        'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token'
    )
    spotify_oauth2_connector.get_access_token()
    mocked_oauth2_get_access_token.assert_called_once()


def test_connect(mocker, spotify_oauth2_connector, spotify_oauth2_data_source):
    """
    Check that connect method correctly provide connection args to spotify.connect
    """
    mocked_connect = mocker.patch.object(
        SpotifyConnector,
        'connect',
        return_value=SpotifyOAuth(
            client_id=spotify_oauth2_connector.client_id,
            client_secret=spotify_oauth2_connector.client_secret,
            redirect_uri=spotify_oauth2_connector.redirect_uri,
            scope=spotify_oauth2_connector.scope,
        ),
    )

    assert spotify_oauth2_connector.connect() == mocked_connect()


def test__retrieve_data(mocker, spotify_oauth2_connector, spotify_oauth2_data_source):
    """Check that the connector is able to retrieve data from Snowflake db/warehouse"""
    mocker.patch.object(SpotifyConnector, 'get_access_token', return_value='shiny token')
    mocked_connect = mocker.patch.object(SpotifyConnector, 'connect')
    mocked_connect._retrieve_data()
