import time

import pandas as pd
import pytest
from mock import patch
from pandas import DataFrame
from snowflake.connector import SnowflakeConnection

from toucan_connectors import DataSlice
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector, SecretsKeeper
from toucan_connectors.snowflake_common import SnowflakeCommon
from toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector import (
    SnowflakeoAuth2Connector,
    SnowflakeoAuth2DataSource,
)


@pytest.fixture
def snowflake_oauth2_connector():
    class LocalSecretsKeeper(SecretsKeeper):
        def save(self, key: str, value: dict):
            return True

        def load(self, key: str):
            return False

    return SnowflakeoAuth2Connector(
        name='snowflake',
        secrets_keeper=LocalSecretsKeeper(),
        auth_flow_id='snowflake',
        authorization_url='AUTHORIZATION_URL',
        redirect_uri='REDIRECT_URI',
        scope='refresh_token',
        client_id='CLIENT_ID',
        client_secret='CLIENT_SECRET',
        role='BENCHMARK_ANALYST',
        default_warehouse='warehouse_1',
        account='toucantocopartner.west-europe.azure',
        identifier='small_app_test' + '_' + 'snowflake',
    )


@pytest.fixture
def snowflake_oauth2_datasource():
    return SnowflakeoAuth2DataSource(
        name='test_name',
        domain='test_domain',
        database='database_1',
        warehouse='warehouse_1',
        query='test_query with %(foo)s and %(pokemon)s',
        parameters={'foo': 'bar', 'pokemon': 'pikachu'},
    )


data = {
    '1 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '2 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '3 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '4 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '5 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '6 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '7 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '8 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '9 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
    '10 Column Name': [
        '1 value',
        '2 value',
        '3 value',
        '4 value',
        '5 value',
        '6 value',
        '7 value',
        '8 value',
        '9 value',
        '10 value',
        ...,
    ],
}
df = pd.DataFrame(
    data,
    columns=[
        '1 Column Name',
        '2 Column Name',
        '3 Column Name',
        '4 Column Name',
        '5 Column Name',
        '6 Column Name',
        '7 Column Name',
        '8 Column Name',
        '9 Column Name',
        '10 Column Name',
        ...,
    ],
)


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon.get_databases',
    return_value=['database_1', 'database_2'],
)
def test_get_database_without_filter(gd, gc, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_databases()
    assert result[0] == 'database_1'
    assert result[1] == 'database_2'
    assert len(result) == 2
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=['database_1']
)
def test_get_database_with_filter_found(gd, gc, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_databases('database_1')
    assert result[0] == 'database_1'
    assert len(result) == 1
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=[])
def test_get_database_with_filter_not_found(gd, gc, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_databases('database_3')
    assert len(result) == 0
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses',
    return_value=['warehouse_1', 'warehouse_2'],
)
def test_get_warehouse_without_filter(gw, gc, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_warehouses()
    assert result[0] == 'warehouse_1'
    assert result[1] == 'warehouse_2'
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses',
    return_value=['warehouse_1'],
)
def test_get_warehouse_with_filter_found(gw, gc, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_warehouses('warehouse_1')
    assert result[0] == 'warehouse_1'
    assert len(result) == 1
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=[])
def test_get_warehouse_with_filter_not_found(gw, gc, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_warehouses('warehouse_3')
    assert len(result) == 0
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch('toucan_connectors.snowflake_common.SnowflakeCommon._execute_query', return_value=df)
def test_retrieve_data(eq, gc, snowflake_oauth2_connector, snowflake_oauth2_datasource, mocker):
    spy = mocker.spy(SnowflakeCommon, '_execute_query')
    df_result: DataFrame = snowflake_oauth2_connector._retrieve_data(snowflake_oauth2_datasource)
    assert spy.call_count == 1
    assert 11 == len(df_result)
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch('toucan_connectors.snowflake_common.SnowflakeCommon._execute_query', return_value=df)
def test_retrieve_data_slice(
    eq, gc, snowflake_oauth2_connector, snowflake_oauth2_datasource, mocker
):
    spy = mocker.spy(SnowflakeCommon, '_execute_query')
    df_result: DataSlice = snowflake_oauth2_connector.get_slice(
        snowflake_oauth2_datasource, offset=0, limit=10
    )
    assert spy.call_count == 1
    assert 11 == len(df_result.df)
    assert 11 == df_result.total_count
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch('toucan_connectors.snowflake_common.SnowflakeCommon._execute_query', return_value=df)
def test_retrieve_data_slice_with_limit(
    eq, gc, snowflake_oauth2_connector, snowflake_oauth2_datasource, mocker
):
    spy = mocker.spy(SnowflakeCommon, '_execute_query')
    df_result: DataSlice = snowflake_oauth2_connector.get_slice(
        snowflake_oauth2_datasource, offset=5, limit=3
    )
    assert spy.call_count == 1
    assert 3 == len(df_result.df)
    assert 3 == df_result.total_count
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch('toucan_connectors.snowflake_common.SnowflakeCommon._execute_query', return_value=df)
def test_retrieve_data_slice_too_much(
    eq, gc, snowflake_oauth2_connector, snowflake_oauth2_datasource, mocker
):
    spy = mocker.spy(SnowflakeCommon, '_execute_query')
    df_result: DataSlice = snowflake_oauth2_connector.get_slice(
        snowflake_oauth2_datasource, offset=10, limit=20
    )
    assert spy.call_count == 1
    assert 1 == len(df_result.df)
    assert 1 == df_result.total_count
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value={'success': True},
)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses',
    return_value=['warehouse_1', 'warehouse_2'],
)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon.get_databases',
    return_value=['database_1', 'database_2'],
)
def test_datasource_get_form(gd, gw, gc, snowflake_oauth2_connector, snowflake_oauth2_datasource):
    result = SnowflakeoAuth2DataSource.get_form(snowflake_oauth2_connector, {})
    assert 'warehouse_1' == result['properties']['warehouse']['default']
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    'toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeoAuth2Connector._get_connection',
    return_value=SnowflakeConnection,
)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon.get_databases',
    return_value=['database_1', 'database_2'],
)
def test_datasource_get_databases(
    gd, connect, snowflake_oauth2_connector, snowflake_oauth2_datasource
):
    result = SnowflakeoAuth2DataSource._get_databases(snowflake_oauth2_connector)
    assert len(result) == 2
    assert result[1] == 'database_2'
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch('snowflake.connector.connect', return_value=SnowflakeConnection)
@patch('snowflake.connector.SnowflakeConnection.close', return_value=None)
@patch('snowflake.connector.SnowflakeConnection.is_closed', return_value=True)
@patch(
    'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token',
    return_value='shiny token',
)
def test_snowflake_connect(gat, is_closed, close, connect, snowflake_oauth2_connector):
    snowflake_oauth2_connector._get_connection('test_database', 'test_warehouse')
    assert gat.call_count == 1
    assert connect.call_args_list[0][1]['account'] == 'toucantocopartner.west-europe.azure'
    assert connect.call_args_list[0][1]['role'] == 'BENCHMARK_ANALYST'
    assert connect.call_args_list[0][1]['token'] == 'shiny token'
    assert connect.call_args_list[0][1]['database'] == 'test_database'
    assert connect.call_args_list[0][1]['warehouse'] == 'test_warehouse'
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch('snowflake.connector.connect', return_value=SnowflakeConnection)
@patch('snowflake.connector.SnowflakeConnection.close', return_value=None)
@patch('snowflake.connector.SnowflakeConnection.is_closed', return_value=True)
@patch(
    'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token',
    return_value='shiny token',
)
def test_snowflake_connection_alive(
    gat, is_closed, close, connect, snowflake_oauth2_connector, mocker
):
    snowflake_oauth2_connector._get_connection('test_database', 'test_warehouse')
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    spy = mocker.spy(SnowflakeConnection, 'is_closed')
    time.sleep(1.1)
    assert spy.call_count == 1
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch('snowflake.connector.connect', return_value=SnowflakeConnection)
@patch('snowflake.connector.SnowflakeConnection.close', return_value=None)
@patch('snowflake.connector.SnowflakeConnection.is_closed', return_value=True)
@patch(
    'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token',
    return_value='shiny token',
)
def test_snowflake_connection_close(
    gat, is_closed, close, connect, snowflake_oauth2_connector, mocker
):
    snowflake_oauth2_connector._get_connection('test_database', 'test_warehouse')
    spy = mocker.spy(SnowflakeConnection, 'close')
    time.sleep(2)
    assert spy.call_count > 1
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


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
