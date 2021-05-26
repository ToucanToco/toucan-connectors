import copy
from datetime import datetime, timedelta
from unittest.mock import call

import jwt
import pandas as pd
import pytest
import responses
import snowflake.connector
from pydantic import SecretStr
from requests.models import HTTPError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.postgres.postgresql_connector import PostgresConnector
from toucan_connectors.snowflake import (
    AuthenticationMethod,
    SnowflakeConnector,
    SnowflakeDataSource,
)
from toucan_connectors.toucan_connector import needs_sso_credentials

sc = SnowflakeConnector(
    name='test_name',
    authentication_method=AuthenticationMethod.PLAIN,
    user='test_user',
    password='test_password',
    account='test_account',
    default_warehouse='default_wh',
)


OAUTH_TOKEN_ENDPOINT = 'http://example.com/endpoint'
OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE = 'application/x-www-form-urlencoded'
OAUTH_ACCESS_TOKEN = jwt.encode({'exp': 42, 'sub': 'snowflake_user'}, key='clef')
OAUTH_REFRESH_TOKEN = 'baba au rhum'
OAUTH_CLIENT_ID = 'client_id'
OAUTH_CLIENT_SECRET = 'client_s3cr3t'


@pytest.fixture
def sc_oauth(mocker):
    user_tokens_keeper = mocker.Mock(
        access_token=SecretStr(OAUTH_ACCESS_TOKEN),
        refresh_token=SecretStr(OAUTH_REFRESH_TOKEN),
        update_tokens=mocker.Mock(),
    )
    sso_credentials_keeper = mocker.Mock(
        client_id=OAUTH_CLIENT_ID,
        client_secret=SecretStr(OAUTH_CLIENT_SECRET),
    )
    return SnowflakeConnector(
        name='test_name',
        authentication_method=AuthenticationMethod.OAUTH,
        user='test_user',
        password='test_password',
        account='test_account',
        token_endpoint=OAUTH_TOKEN_ENDPOINT,
        token_endpoint_content_type=OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE,
        user_tokens_keeper=user_tokens_keeper,
        sso_credentials_keeper=sso_credentials_keeper,
        default_warehouse='default_wh',
    )


sd = SnowflakeDataSource(
    name='test_name',
    domain='test_domain',
    database='test_database',
    warehouse='test_warehouse',
    query='test_query with %(foo)s and %(pokemon)s',
    parameters={'foo': 'bar', 'pokemon': 'pikachu'},
)


@pytest.fixture
def snowflake_connector():
    return SnowflakeConnector(
        name='test_name',
        authentication_method=AuthenticationMethod.PLAIN,
        user='test_user',
        password='test_password',
        account='test_account',
        default_warehouse='default_wh',
    )


@pytest.fixture
def snowflake_datasource():
    return SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        database='test_database',
        warehouse='test_warehouse',
        query='test_query with %(foo)s and %(pokemon)s',
        parameters={'foo': 'bar', 'pokemon': 'pikachu'},
    )


@pytest.fixture
def snowflake_connection_mock(mocker):
    return mocker.patch('snowflake.connector.connect')


@pytest.fixture
def execute_query_mock(mocker):
    return mocker.patch.object(SnowflakeConnector, '_execute_query')


def test_snowflake(mocker):
    connect_mock = mocker.patch('snowflake.connector.connect')

    sc.get_df(sd)

    expected_calls = [
        call('test_query with ? and ?', ['bar', 'pikachu']),
    ]

    mock_execute = connect_mock.return_value.__enter__.return_value.cursor.return_value.execute

    mock_execute.assert_has_calls(expected_calls)

    mock_execute.return_value.fetchall.assert_called_once()

    connect_mock.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        database='test_database',
        warehouse='test_warehouse',
        authenticator=AuthenticationMethod.PLAIN,
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
        authenticator=AuthenticationMethod.PLAIN,
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

    expected_calls = [call('foo', [])]

    snow_mock.return_value.__enter__.return_value.cursor.return_value.execute.assert_has_calls(
        expected_calls
    )

    snow_mock.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        database='db',
        warehouse='default_wh',
        authenticator=AuthenticationMethod.PLAIN,
        application='ToucanToco',
        role=None,
    )


@responses.activate
def test_snowflake_oauth_auth(mocker, sc_oauth):
    responses.add(
        responses.POST,
        OAUTH_TOKEN_ENDPOINT,
        json={'access_token': 'aaa', 'refresh_token': 'bbb'},
    )

    snow_mock = mocker.patch('snowflake.connector.connect')

    sc_oauth.get_df(sd)

    snow_mock.assert_called_once_with(
        user='test_user',
        account='test_account',
        authenticator=AuthenticationMethod.OAUTH,
        database='test_database',
        warehouse='test_warehouse',
        token=sc_oauth.user_tokens_keeper.access_token.get_secret_value(),
        application='ToucanToco',
        role=None,
    )


def test_snowflake_plain_auth(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')

    sc.get_df(sd)

    snow_mock.assert_called_once_with(
        user='test_user',
        account='test_account',
        authenticator=AuthenticationMethod.PLAIN,
        password='test_password',
        database='test_database',
        warehouse='test_warehouse',
        application='ToucanToco',
        role=None,
    )


def test_snowflake_execute_select_query(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')
    cursor_mock = (
        snow_mock.return_value.__enter__.return_value.cursor.return_value.execute.return_value
    )

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


def test_snowflake_execute_select_query_with_params(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')

    connector = SnowflakeConnector(
        name='test', user='test', password='test', account='test', default_warehouse='default_wh'
    )

    data_source = SnowflakeDataSource(
        name='test',
        domain='test',
        database='test',
        warehouse='test',
        query='SELECT * FROM %(formatme)s;',
        parameters={
            'nested': {'dictionary': 'ohno'},
            'formatme': 'ohyeah',
            'no_array': [{'no': 'nope'}],
        },
    )

    connector.get_df(data_source)

    snow_mock.return_value.__enter__.return_value.cursor.return_value.execute.assert_called_with(
        'SELECT * FROM ?;', ['ohyeah']
    )


def test_snowflake_execute_select_query_with_params_jinja_syntax(mocker):
    """It should convert jinja templating to printf templating"""
    snow_mock = mocker.patch('snowflake.connector.connect')
    connector = SnowflakeConnector(
        name='test', user='test', password='test', account='test', default_warehouse='default_wh'
    )
    data_source = SnowflakeDataSource(
        name='test',
        domain='test',
        database='test',
        warehouse='test',
        query='SELECT * FROM {{ formatme }};',
        parameters={
            'nested': {'dictionary': 'ohno'},
            'formatme': 'ohyeah',
            'no_array': [{'no': 'nope'}],
        },
    )

    connector.get_df(data_source)

    snow_mock.return_value.__enter__.return_value.cursor.return_value.execute.assert_called_with(
        'SELECT * FROM ?;', ['ohyeah']
    )


def test_snowflake_execute_other_query(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')
    cursor_mock = (
        snow_mock.return_value.__enter__.return_value.cursor.return_value.execute.return_value
    )

    connector = SnowflakeConnector(
        name='test',
        account='test',
        authentication_method=AuthenticationMethod.PLAIN,
        user='test',
        password='test',
        default_warehouse='default_wh',
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
    )


def test_specified_oauth_args(mocker, sc_oauth):
    mocker.patch('snowflake.connector.connect')

    sc_oauth.user_tokens_keeper.access_token = SecretStr(
        jwt.encode({'exp': datetime.now() - timedelta(hours=24), 'sub': 'user'}, key='supersecret')
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

    sc_oauth._retrieve_data(data_source)

    url, kwargs = req_mock.call_args_list[0]
    assert req_mock.call_count == 1
    assert OAUTH_TOKEN_ENDPOINT == url[0]
    assert OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE == kwargs['headers']['Content-Type']
    assert OAUTH_CLIENT_ID == kwargs['data']['client_id']
    assert OAUTH_CLIENT_SECRET == kwargs['data']['client_secret']


def test_oauth_args_missing_endpoint(mocker, sc_oauth):
    mocker.patch('snowflake.connector.connect')
    req_mock = mocker.patch('requests.post')

    sc_oauth.token_endpoint = None

    sc_oauth._retrieve_data(sd)

    assert req_mock.call_count == 0


@responses.activate
def test_oauth_refresh_token(mocker, sc_oauth):
    mocker.patch('snowflake.connector.connect')

    new_token = jwt.encode(
        {'access_token': 'baba_au_rhum', 'sub': 'mon_super_user'}, key='supersecret'
    )

    responses.add(
        responses.POST,
        OAUTH_TOKEN_ENDPOINT,
        json={'access_token': new_token, 'refresh_token': 'bbb'},
    )

    sc_oauth._retrieve_data(sd)
    assert sc_oauth.user_tokens_keeper.update_tokens.call_count == 1
    assert sc_oauth.user_tokens_keeper.update_tokens.call_args[1]['access_token'] == new_token


def test_schema_fields_order():
    schema_props_keys = list(
        JsonWrapper.loads(SnowflakeConnector.schema_json())['properties'].keys()
    )
    ordered_keys = [
        'type',
        'name',
        'account',
        'authentication_method',
        'user',
        'password',
        'token_endpoint',
        'token_endpoint_content_type',
        'role',
        'default_warehouse',
        'retry_policy',
        'secrets_storage_version',
        'sso_credentials_keeper',
        'user_tokens_keeper',
    ]
    assert schema_props_keys == ordered_keys


def test_oauth_args_endpoint_not_200(mocker, sc_oauth):
    mocker.patch('snowflake.connector.connect')
    req_mock = mocker.patch('requests.post')

    sc_oauth.user_tokens_keeper.access_token = SecretStr(
        jwt.encode({'exp': datetime.now() - timedelta(hours=24)}, key='supersecret')
    )

    req_mock.return_value.status_code = 401

    def fake_raise_for_status():
        raise HTTPError('Unauthorized')

    req_mock.return_value.ok = False
    req_mock.return_value.raise_for_status = lambda: fake_raise_for_status()

    try:
        sc_oauth._retrieve_data(sd)
    except Exception as e:
        assert str(e) == 'Unauthorized'
        assert req_mock.call_count == 1
    else:
        assert False


def test_oauth_args_wrong_type_of_auth(mocker, sc_oauth):
    mocker.patch('snowflake.connector.connect')

    sc_oauth.authentication_method = AuthenticationMethod.PLAIN
    spy = mocker.spy(SnowflakeConnector, '_refresh_oauth_token')

    sc_oauth._retrieve_data(sd)

    assert spy.call_count == 0


def test_get_status_all_good(
    snowflake_connector: SnowflakeConnector, snowflake_connection_mock, execute_query_mock
):
    execute_query_mock.return_value = pd.DataFrame({'warehouse_name': 'default_wh'}, index=[0])
    assert snowflake_connector.get_status() == ConnectorStatus(
        status=True, details=[('Connection to Snowflake', True), ('Default warehouse exists', True)]
    )


def test_get_status_warehouse_does_not_exists(
    snowflake_connector: SnowflakeConnector, snowflake_connection_mock, execute_query_mock
):
    execute_query_mock.return_value = pd.DataFrame()
    assert snowflake_connector.get_status() == ConnectorStatus(
        status=False,
        error="The warehouse 'default_wh' does not exist.",
        details=[('Connection to Snowflake', True), ('Default warehouse exists', False)],
    )


def test_account_does_not_exists(
    snowflake_connector: SnowflakeConnector,
    snowflake_connection_mock,
):
    snowflake_connection_mock.side_effect = snowflake.connector.errors.OperationalError()

    assert snowflake_connector.get_status() == ConnectorStatus(
        status=False,
        error=f"Connection failed for the account '{snowflake_connector.account}', please check the Account field",
        details=[('Connection to Snowflake', False), ('Default warehouse exists', None)],
    )


def test_account_forbidden(snowflake_connector: SnowflakeConnector, snowflake_connection_mock):
    snowflake_connection_mock.side_effect = snowflake.connector.errors.ForbiddenError()

    assert snowflake_connector.get_status() == ConnectorStatus(
        status=False,
        error=f"Access forbidden, please check that you have access to the '{snowflake_connector.account}' account or try again later.",
        details=[('Connection to Snowflake', False), ('Default warehouse exists', None)],
    )


def test_get_status_credentials_nok(
    snowflake_connector: SnowflakeConnector, snowflake_connection_mock
):
    snowflake_connection_mock.side_effect = snowflake.connector.errors.DatabaseError()

    assert snowflake_connector.get_status() == ConnectorStatus(
        status=False,
        error="Connection failed for the user 'test_user', please check your credentials",
        details=[('Connection to Snowflake', False), ('Default warehouse exists', None)],
    )


def test_get_status_account_nok(
    snowflake_connector: SnowflakeConnector, snowflake_connection_mock, execute_query_mock
):
    execute_query_mock.side_effect = snowflake.connector.errors.ProgrammingError('Account nok')

    assert snowflake_connector.get_status() == ConnectorStatus(
        status=False,
        error='Account nok',
        details=[('Connection to Snowflake', False), ('Default warehouse exists', None)],
    )


def test_needs_sso_credentials():
    assert needs_sso_credentials(SnowflakeConnector)
    assert not needs_sso_credentials(PostgresConnector)


def test_get_slice(mocker, snowflake_connector):
    connect_mock = mocker.patch('snowflake.connector.connect')
    cursor_mock = (
        connect_mock.return_value.__enter__.return_value.cursor.return_value.execute.return_value
    )
    ds = SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        database='test_database',
        warehouse='test_warehouse',
        query='select * from buz',
        parameters={'foo': 'bar', 'pokemon': 'pikachu'},
    )
    snowflake_connector.get_slice(ds, None, offset=0, limit=1)

    cursor_mock.fetchmany.assert_called_once_with(1)


def test_get_slice_with_offset(mocker, snowflake_connector):
    connect_mock = mocker.patch('snowflake.connector.connect')
    cursor_mock = (
        connect_mock.return_value.__enter__.return_value.cursor.return_value.execute.return_value
    )
    ds = SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        database='test_database',
        warehouse='test_warehouse',
        query='select * from buz',
        parameters={'foo': 'bar', 'pokemon': 'pikachu'},
    )
    snowflake_connector.get_slice(ds, None, offset=100, limit=10)

    cursor_mock.fetchmany.assert_called_once_with(110)
