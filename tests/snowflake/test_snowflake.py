import pytest

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
    oauth_token='tUh7G0lJs6TjjGzVkv5DAOD4cSFPK5o2',
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


def test_snowflake(mocker):
    snock = mocker.patch('snowflake.connector.connect')
    reasq = mocker.patch('pandas.read_sql')

    sc.get_df(sd)

    snock.return_value.cursor.return_value.execute.assert_called_once_with(
        'USE WAREHOUSE test_warehouse'
    )

    snock.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        database='test_database',
        warehouse='test_warehouse',
        authenticator='snowflake',
        ocsp_response_cache_filename=None,
    )

    reasq.assert_called_once_with('test_query with bar and pikachu', con=snock())


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


def test_snowflake_data_source_default_warehouse(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')
    # Avoid to call read_sql for nothing but no tests required here
    mocker.patch('pandas.read_sql')

    ds = SnowflakeDataSource(name='test', domain='test', database='db', query='foo', warehouse='')

    sc.get_df(ds)

    snow_mock.return_value.cursor.return_value.execute.assert_called_once_with(
        'USE WAREHOUSE default_wh'
    )

    snow_mock.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        database='db',
        warehouse='default_wh',
        ocsp_response_cache_filename=None,
        authenticator='snowflake',
    )


def test_snowflake_oauth_auth(mocker):
    snow_mock = mocker.patch('snowflake.connector.connect')

    sc_oauth.get_df(sd)

    snow_mock.assert_called_once_with(
        user='test_user',
        account='test_account',
        authenticator=AuthenticationMethod.OAUTH,
        database='test_database',
        warehouse='test_warehouse',
        token='tUh7G0lJs6TjjGzVkv5DAOD4cSFPK5o2',
        ocsp_response_cache_filename=None,
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
    )


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
