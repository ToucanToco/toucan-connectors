import pandas as pd
import pytest
import snowflake.connector
from mock import patch

from toucan_connectors import DataSlice
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.snowflake import SnowflakeDataSource
from toucan_connectors.snowflake_common import SnowflakeCommon


@pytest.fixture
def snowflake_datasource():
    return SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        database='database_1',
        warehouse='warehouse_1',
        query='test_query with %(foo)s and %(pokemon)s',
        parameters={'foo': 'bar', 'pokemon': 'pikachu'},
    )


data_result_none = []
data_result_one = [
    {
        '1 Column Name': 'value',
        '2 Column Name': 'value',
        '3 Column Name': 'value',
        '4 Column Name': 'value',
        '5 Column Name': 'value',
        '6 Column Name': 'value',
        '7 Column Name': 'value',
        '8 Column Name': 'value',
        '9 Column Name': 'value',
        '10 Column Name': 'value',
        '11 Column Name': 'value',
    }
]
data_result_5 = JsonWrapper.load(
    open(
        'tests/fixtures/fixture_snowflake_common/data_5.json',
    )
)
data_result_all = JsonWrapper.load(
    open(
        'tests/fixtures/fixture_snowflake_common/data_10.json',
    )
)
databases_result_all = [{'name': 'database_1'}, {'name': 'database_2'}]
databases_result_none = []
databases_result_one = [{'name': 'database_1'}]
warehouses_result_all = [{'name': 'warehouse_1'}, {'name': 'warehouse_2'}]
warehouses_result_none = []
warehouses_result_one = [{'name': 'warehouse_1'}]


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute', return_value=None)
@patch('pandas.DataFrame.from_dict', return_value=databases_result_all)
def test_get_database_without_filter(database_result, execute_query, connect):
    result = SnowflakeCommon().get_databases(connect)
    assert database_result.call_count == 1
    assert result[0] == 'database_1'
    assert result[1] == 'database_2'
    assert len(result) == 2


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=databases_result_none)
def test_get_database_with_filter_no_result(database_result, execute_query, connect, mocker):
    result = SnowflakeCommon().get_databases(connect, 'database_3')
    assert database_result.call_count == 1
    assert len(result) == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=databases_result_one)
def test_get_database_with_filter_one_result(database_result, execute_query, connect, mocker):
    result = SnowflakeCommon().get_databases(connect, 'database_1')
    assert database_result.call_count == 1
    assert result[0] == 'database_1'
    assert len(result) == 1


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=warehouses_result_all)
def test_get_warehouse_without_filter(warehouse_result, execute_query, connect, mocker):
    result = SnowflakeCommon().get_warehouses(connect)
    assert warehouse_result.call_count == 1
    assert result[0] == 'warehouse_1'
    assert result[1] == 'warehouse_2'
    assert len(result) == 2


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=warehouses_result_none)
def test_get_warehouse_with_filter_no_result(warehouse_result, execute_query, connect, mocker):
    result = SnowflakeCommon().get_warehouses(connect, 'warehouse_3')
    assert warehouse_result.call_count == 1
    assert len(result) == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=warehouses_result_one)
def test_get_warehouse_with_filter_one_result(warehouse_result, execute_query, connect, mocker):
    result = SnowflakeCommon().get_warehouses(connect, 'warehouse_1')
    assert warehouse_result.call_count == 1
    assert result[0] == 'warehouse_1'
    assert len(result) == 1


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
def test_retrieve_data(result, execute_query, connect, snowflake_datasource, mocker):
    df: pd.DataFrame = SnowflakeCommon()._retrieve_data(connect, snowflake_datasource)
    assert result.call_count == 1
    assert len(df) == 14


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
def test_get_slice_without_limit_without_offset(
    result, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert result.call_count == 1
    assert len(df.df) == 14
    assert df.total_count == 14


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_5))
def test_get_slice_with_limit_without_offset(
    result, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 1
    assert len(df.df) == 5
    assert df.total_count == 5


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_none))
def test_get_slice_with_limit_without_offset_no_data(
    resut, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert resut.call_count == 1
    assert len(df.df) == 0
    assert df.total_count == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_one))
def test_get_slice_with_limit_without_offset_not_enough_data(
    result, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 1
    assert len(df.df) == 1
    assert df.total_count == 1


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
def test_get_slice_with_limit_with_offset(
    result, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 1
    assert len(df.df) == 5
    assert df.total_count == 5


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_none))
def test_get_slice_with_limit_with_offset_no_data(
    result, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 1
    assert len(df.df) == 0
    assert df.total_count == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_one))
def test_get_slice_with_limit_with_offset_not_enough_data(
    result, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 1
    assert len(df.df) == 0
    assert df.total_count == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
def test_get_slice_without_limit_with_offset(
    result, execute_query, connect, snowflake_datasource, mocker
):
    df: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5)
    assert result.call_count == 1
    assert len(df.df) == 14
    assert df.total_count == 14
