from pathlib import Path

from pytest import fixture

from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector
from toucan_connectors.snowflakeids.snowflakeids_connector import (
    SnowflakeIdsConnector,
    SnowflakeIdsDataSource,
)

import_path = 'toucan_connectors.snowflakeids.snowflakeids_connector'


@fixture
def con(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'access_token'})
    return SnowflakeIdsConnector(
        name='test',
        auth_flow_id='test',
        client_id='CLIENT_ID',
        client_secret='CLIENT_SECRET',
        redirect_uri='REDIRECT_URI',
        secrets_keeper=secrets_keeper,
        account='acc',
    )


@fixture
def remove_secrets(secrets_keeper, con):
    secrets_keeper.save('test', {'access_token': None})


@fixture
def ds():
    return SnowflakeIdsDataSource(
        name='test_name',
        domain='test_domain',
        database='db',
        warehouse='wh',
        query='select * from test;',
    )


def test_snowflake__get_databases(mocker, con, ds):
    mockconnect = mocker.patch('snowflake.connector')
    mockconnect.connect().__enter__().cursor().execute().fetchall.return_value = [{'name': 'db'}]
    res = ds._get_databases(con)
    assert res == ['db']


def test_snowflake__get_warehousess(mocker, con):
    mockconnect = mocker.patch('snowflake.connector')
    mockconnect.connect().__enter__().cursor().execute().fetchall.return_value = [{'name': 'wh'}]
    res = con._get_warehouses()
    assert res == ['wh']


def test_delegate_oauth2_methods(mocker, con):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    con.__dict__['_oauth2_connector'] = mock_oauth2_connector
    con.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()
    con.retrieve_tokens('toto')
    mock_oauth2_connector.retrieve_tokens.assert_called_with('toto')


def test_connect(mocker, con):
    """
    It should setup & return a connection object
    """
    mock_snowflake_connect = mocker.patch('snowflake.connector.connect')
    con.connect()
    assert mock_snowflake_connect.call_args[1] == {
        'account': 'acc',
        'authenticator': 'oauth',
        'application': 'ToucanToco',
        'token': 'access_token',
    }


def test__execute_query(mocker, con):
    """
    It should call fetch_pandas_all with correct args
    """
    mock_connect = mocker.patch(f'{import_path}.SnowflakeIdsConnector.connect')
    mocked_curs = mock_connect.cursor()
    con._execute_query(mocked_curs, 'select * from blah;', {})
    mocked_curs.execute().fetch_pandas_all.assert_called_once()


def test__retrieve_data(mocker, con, ds):
    """
    It should call _execute_query with correct args
    """
    mocked_execute = mocker.patch(f'{import_path}.SnowflakeIdsConnector._execute_query')
    mock_snowflake_connect = mocker.patch('snowflake.connector')
    mocked_curs = mock_snowflake_connect.connect().cursor().__enter__()
    con._retrieve_data(ds)
    assert mocked_execute.call_args_list[0][0] == (mocked_curs, 'select * from test;', None)


def test_get_secrets_form(mocker, con):
    """Check that the doc for oAuth setup is correctly retrieved"""
    mocker.patch(f'{import_path}.os.path.dirname', return_value='fakepath')
    mocker.patch.object(Path, 'read_text', return_value='<h1>Awesome Doc</h1>')
    doc = con.get_connector_secrets_form()
    assert doc.documentation_md == '<h1>Awesome Doc</h1>'
