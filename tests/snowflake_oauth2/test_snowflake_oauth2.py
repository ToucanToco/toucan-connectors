from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector
from toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector import SnowflakeoAuth2Connector


def test_build_authorization_url(mocker, snowflake_oauth2_connector):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    snowflake_oauth2_connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    snowflake_oauth2_connector.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_tokens(mocker, snowflake_oauth2_connector):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    snowflake_oauth2_connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    snowflake_oauth2_connector.retrieve_tokens('bla')
    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_get_access_token(mocker, snowflake_oauth2_connector):
    """
    Check that get_access correctly calls oAuth2Connector's get_access_token
    """
    mocked_oauth2_get_access_token = mocker.patch(
        'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token'
    )
    snowflake_oauth2_connector.get_access_token()
    mocked_oauth2_get_access_token.assert_called_once()


def test_connect(mocker, snowflake_oauth2_connector, snowflake_data_source):
    """
    Check that connect method correctly provide connection args to pysnowflake.connect
    """
    mocked_connect = mocker.patch('snowflake.connector.connect')
    mocked_get_access = mocker.patch.object(
        SnowflakeoAuth2Connector, 'get_access_token', return_value='shiny token'
    )
    snowflake_oauth2_connector.connect(database='test_database', warehouse='test_warehouse')
    assert mocked_connect.call_args_list[0][1]['account'] == 'acc'
    assert mocked_connect.call_args_list[0][1]['database'] == 'test_database'
    assert mocked_connect.call_args_list[0][1]['warehouse'] == 'test_warehouse'
    assert mocked_connect.call_args_list[0][1]['role'] == 'PUBLIC'
    assert mocked_connect.call_args_list[0][1]['token'] == 'shiny token'
    assert mocked_get_access.call_count == 1


def test__retrieve_data(mocker, snowflake_oauth2_connector, snowflake_data_source):
    """Check that the connector is able to retrieve data from Snowflake db/warehouse"""
    mocker.patch.object(SnowflakeoAuth2Connector, 'get_access_token', return_value='shiny token')
    mocked_connect = mocker.patch.object(SnowflakeoAuth2Connector, 'connect')
    snowflake_oauth2_connector._retrieve_data(snowflake_data_source)
    assert mocked_connect.call_args_list[0][1] == {
        'database': 'test_database',
        'warehouse': 'test_warehouse',
    }


def test_get__warehouses(mocker, snowflake_oauth2_connector):
    """Check that _get_warehouses correctly connects & retrieve a warehouse list"""
    mocked_connect = mocker.patch('snowflake.connector.connect')
    mocker.patch.object(SnowflakeoAuth2Connector, 'get_access_token', return_value='shiny token')
    snowflake_oauth2_connector._get_warehouses()
    assert mocked_connect.call_args_list[0][1]['account'] == 'acc'
    assert mocked_connect.call_args_list[0][1]['database'] is None
    assert mocked_connect.call_args_list[0][1]['warehouse'] is None
    assert mocked_connect.call_args_list[0][1]['role'] == 'PUBLIC'
    assert mocked_connect.call_args_list[0][1]['token'] == 'shiny token'
