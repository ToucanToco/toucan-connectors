from sqlite3 import ProgrammingError

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
        query='select * from my_table where toto=%(foo);',
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
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(databases_result_all),
)
def test_get_database_without_filter(database_result, execute_query, connect):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(databases_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
def test_get_database_without_filter(database_result, execute_query, connect, mocker):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    result = SnowflakeCommon().get_databases(connect)
    assert database_result.call_count == 1
    assert result[0] == 'database_1'
    assert result[1] == 'database_2'
    assert len(result) == 2


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(databases_result_none),
)
def test_get_database_with_filter_no_result(database_result, execute_query, connect):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(databases_result_none))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
def test_get_database_with_filter_no_result(database_result, execute_query, connect, mocker):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    result = SnowflakeCommon().get_databases(connect, 'database_3')
    assert database_result.call_count == 1
    assert result is None


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(databases_result_one),
)
def test_get_database_with_filter_one_result(database_result, execute_query, connect):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(databases_result_one))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
def test_get_database_with_filter_one_result(database_result, execute_query, connect, mocker):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    result = SnowflakeCommon().get_databases(connect, 'database_1')
    assert database_result.call_count == 1
    assert result[0] == 'database_1'
    assert len(result) == 1


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(warehouses_result_all),
)
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(warehouses_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
def test_get_warehouse_without_filter(warehouse_result, execute_query, connect, mocker):
    result = SnowflakeCommon().get_warehouses(connect)
    assert warehouse_result.call_count == 1
    assert result[0] == 'warehouse_1'
    assert result[1] == 'warehouse_2'
    assert len(result) == 2


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(warehouses_result_none),
)
def test_get_warehouse_with_filter_no_result(warehouse_result, execute_query, connect):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(warehouses_result_none))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
def test_get_warehouse_with_filter_no_result(warehouse_result, execute_query, connect, mocker):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    result = SnowflakeCommon().get_warehouses(connect, 'warehouse_3')
    assert warehouse_result.call_count == 1
    assert result is None


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(warehouses_result_one),
)
def test_get_warehouse_with_filter_one_result(warehouse_result, execute_query, connect):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(warehouses_result_one))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
def test_get_warehouse_with_filter_one_result(warehouse_result, execute_query, connect, mocker):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    result = SnowflakeCommon().get_warehouses(connect, 'warehouse_1')
    assert warehouse_result.call_count == 1
    assert result[0] == 'warehouse_1'
    assert len(result) == 1


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
def test_retrieve_data(result, execute_query, connect, snowflake_datasource):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
def test_retrieve_data(result, execute_query, connect, snowflake_datasource, mocker):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    df: pd.DataFrame = SnowflakeCommon().retrieve_data(connect, snowflake_datasource)
    assert result.call_count == 1
    assert len(df) == 14


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_without_limit_without_offset(
    result, execute_query, connect, snowflake_datasource
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_without_limit_without_offset(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
):
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert result.call_count == 1
    assert len(slice.df) == 14
    assert slice.stats.total_returned_rows == 14


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_5),
)
def test_get_slice_with_limit_without_offset(result, execute_query, connect, snowflake_datasource):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_5))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_limit_without_offset(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 1
    assert len(slice.df) == 5
    assert slice.stats.total_returned_rows == 5


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_none),
)
def test_get_slice_with_limit_without_offset_no_data(
    resut, execute_query, connect, snowflake_datasource
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_none))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_limit_without_offset_no_data(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
):
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 1
    assert len(slice.df) == 0
    assert slice.stats.total_returned_rows == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_one),
)
def test_get_slice_with_limit_without_offset_not_enough_data(
    result, execute_query, connect, snowflake_datasource
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_one))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_limit_without_offset_not_enough_data(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
):
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 1
    assert len(slice.df) == 1
    assert slice.stats.total_returned_rows == 1


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_with_limit_with_offset(result, execute_query, connect, snowflake_datasource):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_limit_with_offset(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 1
    assert len(slice.df) == 5
    assert slice.stats.total_returned_rows == 5


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_none),
)
def test_get_slice_with_limit_with_offset_no_data(
    result, execute_query, connect, snowflake_datasource
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_none))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_limit_with_offset_no_data(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
):
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 1
    assert len(slice.df) == 0
    assert slice.stats.total_returned_rows == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_one),
)
def test_get_slice_with_limit_with_offset_not_enough_data(
    result, execute_query, connect, snowflake_datasource
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_one))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_limit_with_offset_not_enough_data(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
):
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 1
    assert len(slice.df) == 0
    assert slice.stats.total_returned_rows == 0


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_without_limit_with_offset(result, execute_query, connect, snowflake_datasource):
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_without_limit_with_offset(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
):
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5)
    assert result.call_count == 1
    assert len(slice.df) == 14
    assert slice.stats.total_returned_rows == 14


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_with_limit_extracted_from_query(
    result, execute_query, connect, snowflake_datasource
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit', return_value=12)
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_limit_extracted_from_query(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
):
    snowflake_datasource.query = 'select name from favourite_drinks limit 12;'
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert slice.input_parameters.get('limit') == 12
    snowflake_datasource.query = 'select name from favourite_drinks;'
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert not slice.input_parameters.get('limit')


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_with_offset_extracted_from_query(
    result, execute_query, connect, snowflake_datasource
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_limit')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.extract_offset')
def test_get_slice_with_offset_extracted_from_query(
    offset, limit, prepare_query, fetchmany, result, execute_query, connect, snowflake_datasource,
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
):
    snowflake_datasource.query = 'select name from favourite_drinks limit 12 offset 23;'
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert slice.input_parameters.get('offset') == 23
    snowflake_datasource.query = 'select name from favourite_drinks limit 12;'
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert slice.input_parameters.get('offset') is None


<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
def test_get_slice_metadata(snowflake_datasource, mocker):
    snowflake_datasource.query ='select name from favourite_drinks limit 12 offset 23;'
    connect = mocker.MagicMock()
    connect.cursor().execute().fetchone.return_value = [{'total_rows': 200}]
    connect.cursor().execute().fetchall.return_value = [{'c1': 2}]
    slice: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert slice.stats.df_memory_size == 1360
    assert slice.stats.total_returned_rows == 14


<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
def test_get_slice_metadata_no_select_in_query(result, snowflake_datasource, mocker):
    snowflake_datasource.query =  ('create table users as  (id integer default id_seq.nextval,  name varchar (100), preferences string, '
        'created_at timestamp); ',
    )
    connect = mocker.MagicMock()
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert result.call_count == 1
    assert ds


<<<<<<< HEAD
@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    return_value=pd.DataFrame(data_result_all),
)
=======
@patch('pandas.DataFrame.from_dict', return_value=pd.DataFrame(data_result_all))
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
def test_get_slice_metadata_wrong_response_from_count_query(snowflake_datasource, mocker):
    snowflake_datasource.query = 'select name from favourite_drinks limit 12 offset 23;'
    connect = mocker.MagicMock()
    connect.cursor().execute().fetchone.return_value = [{'error': 'invalid query'}]
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert ds
    connect.cursor().execute().fetchone.side_effect = Exception()
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert ds
    connect.cursor().execute().fetchone.side_effect = IndexError()
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert ds
    connect.cursor().execute().fetchone.side_effect = TypeError()
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert ds
    connect.cursor().execute().fetchone.side_effect = AttributeError()
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert ds


@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon._execute_query',
    side_effect=ProgrammingError,
)
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
def test_execute_broken_query(result, execute_query, snowflake_datasource, mocker):
    snowflake_datasource.query = 'select name from favourite_drinks limit 12 offset 23;'
    connect = mocker.MagicMock()
    with pytest.raises(ProgrammingError):
        SnowflakeCommon()._execute_parallelized_queries(
            connect, snowflake_datasource.query, snowflake_datasource.parameters
        )


@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch(
    'toucan_connectors.snowflake_common.SnowflakeCommon._execute_query',
    side_effect=ProgrammingError,
)
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
def test_count_request_needed(result, execute_query, snowflake_datasource, mocker):
    res: bool = SnowflakeCommon().count_request_needed(snowflake_datasource.query, True, 2)
    assert res
    res: bool = SnowflakeCommon().count_request_needed(snowflake_datasource.query, False)
    assert not res


@patch(
    'toucan_connectors.query_manager.QueryManager.fetchmany',
    side_effect=[pd.DataFrame(data_result_all), pd.DataFrame({'TOTAL_ROWS': 20}, index=[0])],
)
<<<<<<< HEAD
def test_retrieve_data_with_row_count_limit_in_query(fetchmany, snowflake_datasource, mocker):
    snowflake_datasource.query = 'select name from favourite_drinks limit 10;'
=======
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
def test_retrieve_data_with_row_count_limit_in_query(
    result, execute_query, snowflake_datasource, mocker
):
    snowflake_datasource.query = 'select name from favourite_drinks limit 12 offset 23;'
    connect = mocker.MagicMock()
    connect.cursor().execute().rowcount.return_value = 12
    s = SnowflakeCommon()
    s.retrieve_data(connect, snowflake_datasource, get_row_count=True)
    assert result.call_count == 2
    assert s.count == 12


@patch(
    'pandas.DataFrame.from_dict',
    side_effect=[pd.DataFrame(data_result_all), pd.DataFrame({'TOTAL_ROWS': 12}, index=[0])],
)
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.fetchmany')
@patch('toucan_connectors.sql_query_manager.SqlQueryManager.prepare_query', return_value=('foo', 'bar'))
def test_retrieve_data_with_row_count_no_limit_in_query(
    result, execute_query, snowflake_datasource, mocker
):
    snowflake_datasource.query = 'select name from favourite_drinks;'
>>>>>>> chore: move methods to sql_query_manager and implement fetchmany
    connect = mocker.MagicMock()
    SnowflakeCommon().retrieve_data(connect, snowflake_datasource, get_row_count=True)
    assert fetchmany.call_count == 2
