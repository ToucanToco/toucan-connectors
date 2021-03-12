import copy
from datetime import datetime, timedelta
from unittest.mock import call

import jwt
import pytest
from requests.models import HTTPError

from toucan_connectors.snowflake import (
    AuthenticationMethod,
    SnowflakeConnector,
    SnowflakeDataSource,
)

sc = SnowflakeConnector(
    name='test_name',
    authentication_method=AuthenticationMethod.PLAIN,
    user='test_user',
    password='test_password',
    account='test_account',
    default_warehouse='default_wh',
)

sc_oauth = SnowflakeConnector(
    name='test_name',
    authentication_method=AuthenticationMethod.OAUTH,
    user='test_user',
    password='test_password',
    account='test_account',
    oauth_token=jwt.encode({'exp': 42, 'sub': 'snowflake_user'}, key='clef'),
    default_warehouse='default_wh',
)

sd = SnowflakeDataSource(
    name='test_name',
    domain='test_domain',
    database='test_database',
    warehouse='test_warehouse',
    query='test_query with %(foo)s and {{ pokemon }}',
    parameters={'foo': 'bar', 'pokemon': 'pikachu'},
)

OAUTH_ARGS = {
    'content_type': 'application/x-www-form-urlencoded',
    'client_id': 'client_id',
    'client_secret': 'client_s3cr3t',
    'refresh_token': 'baba au rhum',
    'token_endpoint': 'http://example.com/endpoint',
}


def test_snowflake(mocker):
    snock = mocker.patch('snowflake.connector.connect')

    sc.get_df(sd)

    expected_calls = [
        call('test_query with %(foo)s and {{ pokemon }}', {'foo': 'bar', 'pokemon': 'pikachu'}),
    ]

    mock_execute = snock.return_value.cursor.return_value.execute

    mock_execute.assert_has_calls(expected_calls)

    mock_execute.return_value.fetchall.assert_called_once()

    snock.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        database='test_database',
        warehouse='test_warehouse',
        authenticator='snowflake',
        ocsp_response_cache_filename=None,
        application='ToucanToco',
        role=None,
    )


def test_snowflake_custom_role(mocker):
    snock = mocker.patch('snowflake.connector.connect')

    connector = copy.deepcopy(sc)
    connector.role = 'TEST'

    connector.get_df(sd)

    snock.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        database='test_database',
        warehouse='test_warehouse',
        authenticator='snowflake',
        ocsp_response_cache_filename=None,
        application='ToucanToco',
        role='TEST',
    )


def test_snowflake_custom_role_empty(mocker):
    snock = mocker.patch('snowflake.connector.connect')

    connector = copy.deepcopy(sc)
    connector.role = ''

    connector.get_df(sd)

    _, kwargs = snock.call_args_list[0]
    assert 'role' not in kwargs


def test_snowflake_get_connection_params_no_auth_method(mocker):
    res = SnowflakeConnector(
        name='test',
        user='test',
        password='test_password',
        account='test_account',
        default_warehouse='test_wh',
    ).get_connection_params()

    assert res['authenticator'] == AuthenticationMethod.PLAIN


def test_snowflake_data_source_get_form(mocker):
    ds = SnowflakeDataSource(
        name='test',
        domain='blah',
        database='bleh',
        query='foo',
        warehouse='',
    )

    mocker.patch('snowflake.connector.connect')

    sf_form = ds.get_form(sc, {})

    assert 'default_wh' == sf_form['properties']['warehouse']['default']


def test_snowflake_get_form_with_databases(mocker):
    mocker.patch('snowflake.connector.connect')

    data_source = SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        warehouse='test_warehouse',
        database='foo',
        query='foo',
    )

    get_db_mock = mocker.patch('toucan_connectors.snowflake.SnowflakeDataSource._get_databases')
    get_db_mock.return_value = ['foo', 'bar']
    sf_form = data_source.get_form(sc, {})

    get_db_mock.assert_called_once()
    assert sf_form['definitions']['database']['enum'] == ['foo', 'bar']


def test_snowflake_get_form_with_warehouses(mocker):
    mocker.patch('snowflake.connector.connect')

    data_source = SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        warehouse='test_warehouse',
        database='foo',
        query='bar',
    )

    get_warehouses_mock = mocker.patch(
        'toucan_connectors.snowflake.SnowflakeConnector._get_warehouses'
    )
    get_warehouses_mock.return_value = ['foo', 'bar']
    sf_form = data_source.get_form(sc, {})

    get_warehouses_mock.assert_called_once()
    assert sf_form['definitions']['warehouse']['enum'] == ['foo', 'bar']


def test_snowflake_data_source_default_warehouse(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')
    # Avoid to call read_sql for nothing but no tests required here
    mocker.patch('pandas.read_sql')

    ds = SnowflakeDataSource(name='test', domain='test', database='db', query='foo', warehouse='')

    sc.get_df(ds)

    expected_calls = [call('foo', None)]

    snow_mock.return_value.cursor.return_value.execute.assert_has_calls(expected_calls)

    snow_mock.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        database='db',
        warehouse='default_wh',
        ocsp_response_cache_filename=None,
        authenticator='snowflake',
        application='ToucanToco',
        role=None,
    )


def test_snowflake_oauth_auth(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')

    sf = copy.deepcopy(sc_oauth)

    sf.get_df(sd)

    snow_mock.assert_called_once_with(
        user='test_user',
        account='test_account',
        authenticator=AuthenticationMethod.OAUTH,
        database='test_database',
        warehouse='test_warehouse',
        token=sc_oauth.oauth_token,
        ocsp_response_cache_filename=None,
        application='ToucanToco',
        role=None,
    )


def test_snowflake_plain_auth(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')

    sc.get_df(sd)

    snow_mock.assert_called_once_with(
        user='test_user',
        account='test_account',
        password='test_password',
        authenticator=AuthenticationMethod.PLAIN,
        database='test_database',
        warehouse='test_warehouse',
        ocsp_response_cache_filename=None,
        application='ToucanToco',
        role=None,
    )


def test_snowflake_execute_select_query(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')
    cursor_mock = snow_mock.return_value.cursor.return_value.execute.return_value

    connector = SnowflakeConnector(
        name='test', user='test', password='test', account='test', default_warehouse='default_wh'
    )

    data_source = SnowflakeDataSource(
        name='test',
        domain='test',
        database='test',
        warehouse='test',
        query='foo',
    )

    data_source.query = 'SELECT * FROM foo;'

    connector.get_df(data_source)

    data_source.query = 'select * from foo;'

    connector.get_df(data_source)

    assert cursor_mock.fetch_pandas_all.call_count == 2
    cursor_mock.fetchall.assert_not_called()


def test_snowflake_execute_other_query(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')
    cursor_mock = snow_mock.return_value.cursor.return_value.execute.return_value

    connector = SnowflakeConnector(
        name='test', user='test', password='test', account='test', default_warehouse='default_wh'
    )

    data_source = SnowflakeDataSource(
        name='test',
        domain='test',
        database='test',
        warehouse='test',
        query='foo',
    )

    data_source.query = 'SHOW TABLES;'

    connector.get_df(data_source)

    cursor_mock.fetch_pandas_all.assert_not_called()
    cursor_mock.fetchall.assert_called_once()


def test_missing_cache_file():
    with pytest.raises(ValueError):
        SnowflakeConnector(
            user='', password='', account='', name='', ocsp_response_cache_filename='/blah'
        )

    SnowflakeConnector(
        authentication_method=AuthenticationMethod.PLAIN,
        user='',
        password='',
        account='',
        name='',
        default_warehouse='bleh',
        ocsp_response_cache_filename=__file__,
    )


def test_specified_oauth_args(mocker):
    mocker.patch('snowflake.connector.connect')

    sf = copy.deepcopy(sc_oauth)
    sf.oauth_args = copy.deepcopy(OAUTH_ARGS)

    sf.oauth_token = jwt.encode(
        {'exp': datetime.now() - timedelta(hours=24), 'sub': 'user'}, key='supersecret'
    )

    data_source = SnowflakeDataSource(
        name='test',
        domain='test',
        database='test',
        warehouse='test',
        query='bar',
    )

    req_mock = mocker.patch('requests.post')
    req_mock.return_value.status_code = 200
    req_mock.return_value.json = lambda: {
        'access_token': jwt.encode({'exp': datetime.now(), 'sub': 'user'}, key='supersecret')
    }

    sf._retrieve_data(data_source)

    url, kwargs = req_mock.call_args_list[0]
    assert req_mock.call_count == 1
    assert OAUTH_ARGS['token_endpoint'] == url[0]
    assert OAUTH_ARGS['client_id'] == kwargs['data']['client_id']
    assert OAUTH_ARGS['client_secret'] == kwargs['data']['client_secret']
    assert OAUTH_ARGS['content_type'] == kwargs['headers']['Content-Type']


def test_oauth_args_missing_endpoint(mocker):
    mocker.patch('snowflake.connector.connect')
    req_mock = mocker.patch('requests.post')

    sf = copy.deepcopy(sc_oauth)

    oauth_args = copy.deepcopy(OAUTH_ARGS)
    oauth_args.pop('token_endpoint')
    sf.oauth_args = oauth_args

    sf._retrieve_data(sd)

    assert req_mock.call_count == 0


def test_oauth_refresh_token(mocker):
    mocker.patch('snowflake.connector.connect')
    req_mock = mocker.patch('requests.post')
    sf = copy.deepcopy(sc_oauth)
    sf.oauth_args = copy.deepcopy(OAUTH_ARGS)

    req_mock.return_value.json = lambda: {
        'access_token': jwt.encode(
            {'access_token': 'baba_au_rhum', 'sub': 'mon_super_user'}, key='supersecret'
        )
    }

    sf._retrieve_data(sd)

    assert req_mock.call_count == 1
    assert sf.oauth_token == req_mock.return_value.json()['access_token']


def test_oauth_args_endpoint_not_200(mocker):
    mocker.patch('snowflake.connector.connect')
    req_mock = mocker.patch('requests.post')

    sf = copy.deepcopy(sc_oauth)
    oauth_args = copy.deepcopy(OAUTH_ARGS)
    sf.oauth_args = oauth_args
    sf.oauth_token = jwt.encode({'exp': datetime.now() - timedelta(hours=24)}, key='supersecret')

    req_mock.return_value.status_code = 401

    def fake_raise_for_status():
        raise HTTPError('Unauthorized')

    req_mock.return_value.ok = False
    req_mock.return_value.raise_for_status = lambda: fake_raise_for_status()

    try:
        sf._retrieve_data(sd)
    except Exception as e:
        assert str(e) == 'Unauthorized'
        assert req_mock.call_count == 1
    else:
        assert False


def test_oauth_args_wrong_type_of_auth(mocker):
    mocker.patch('snowflake.connector.connect')
    sf = copy.deepcopy(sc)
    sf.oauth_args = copy.deepcopy(OAUTH_ARGS)

    spy = mocker.spy(SnowflakeConnector, '_refresh_oauth_token')

    sf._retrieve_data(sd)

    assert spy.call_count == 0
