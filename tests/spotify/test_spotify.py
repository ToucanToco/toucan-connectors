from toucan_connectors.spotify.spotify_connector import SpotifyConnector, SpotifyDataSource


def test_get_df():
    data_source = SpotifyDataSource(
        name='spotify', domain='https://spotify.com', query='weezer', limit=5
    )

    connector = SpotifyConnector(
        name='spotify',
    )
    connector.client_id = '9dc5eae093274640ba537d928be0db63'
    connector.client_secret = '8c3d809636d54b8db7f83e178accc0a7'

    df = connector.get_df(data_source)

    assert 'items' in df['tracks']
