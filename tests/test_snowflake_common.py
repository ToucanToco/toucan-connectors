from sqlite3 import ProgrammingError
from unittest.mock import patch

import pandas as pd
import pytest
import snowflake.connector

from toucan_connectors import DataSlice
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.pagination import OffsetLimitInfo
from toucan_connectors.snowflake import SnowflakeDataSource
from toucan_connectors.snowflake_common import SnowflakeCommon


@pytest.fixture
def snowflake_datasource():
    return SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_1",
        warehouse="warehouse_1",
        query="select * from my_table where toto=%(foo);",
        query_object={"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]},
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )


data_result_none = []
data_result_one = [
    {
        "1 Column Name": "value",
        "2 Column Name": "value",
        "3 Column Name": "value",
        "4 Column Name": "value",
        "5 Column Name": "value",
        "6 Column Name": "value",
        "7 Column Name": "value",
        "8 Column Name": "value",
        "9 Column Name": "value",
        "10 Column Name": "value",
        "11 Column Name": "value",
    }
]
data_result_5 = JsonWrapper.load(
    open(
        "tests/fixtures/fixture_snowflake_common/data_5.json",
    )
)
data_result_all = JsonWrapper.load(
    open(
        "tests/fixtures/fixture_snowflake_common/data_10.json",
    )
)
databases_result_all = [{"name": "database_1"}, {"name": "database_2"}]
databases_result_none = []
databases_result_one = [{"name": "database_1"}]
warehouses_result_all = [{"name": "warehouse_1"}, {"name": "warehouse_2"}]
warehouses_result_none = []
warehouses_result_one = [{"name": "warehouse_1"}]


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute", return_value=None)
@patch("pandas.DataFrame.from_dict", return_value=pd.DataFrame(databases_result_all))
def test_get_database_without_filter(database_result, execute_query, connect):
    result = SnowflakeCommon().get_databases(connect)
    assert database_result.call_count == 1
    assert result[0] == "database_1"
    assert result[1] == "database_2"
    assert len(result) == 2


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(databases_result_none),
)
def test_get_database_with_filter_no_result(database_result, execute_query, connect):
    result = SnowflakeCommon().get_databases(connect, "database_3")
    assert database_result.call_count == 1
    assert len(result) == 0


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(databases_result_one),
)
def test_get_database_with_filter_one_result(database_result, execute_query, connect):
    result = SnowflakeCommon().get_databases(connect, "database_1")
    assert database_result.call_count == 1
    assert result[0] == "database_1"
    assert len(result) == 1


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(warehouses_result_all),
)
def test_get_warehouse_without_filter(warehouse_result, execute_query, connect, mocker):
    result = SnowflakeCommon().get_warehouses(connect)
    assert warehouse_result.call_count == 1
    assert result[0] == "warehouse_1"
    assert result[1] == "warehouse_2"
    assert len(result) == 2


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(warehouses_result_none),
)
def test_get_warehouse_with_filter_no_result(warehouse_result, execute_query, connect):
    result = SnowflakeCommon().get_warehouses(connect, "warehouse_3")
    assert warehouse_result.call_count == 1
    assert len(result) == 0


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(warehouses_result_one),
)
def test_get_warehouse_with_filter_one_result(warehouse_result, execute_query, connect):
    result = SnowflakeCommon().get_warehouses(connect, "warehouse_1")
    assert warehouse_result.call_count == 1
    assert result[0] == "warehouse_1"
    assert len(result) == 1


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_all),
)
def test_retrieve_data(result, execute_query, connect, snowflake_datasource):
    df: pd.DataFrame = SnowflakeCommon().retrieve_data(connect, snowflake_datasource)
    assert result.call_count == 3
    assert len(df) == 14


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_without_limit_without_offset(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert result.call_count == 3
    assert len(ds.df) == 14
    assert ds.pagination_info.pagination_info.total_rows == 14


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_5),
)
def test_get_slice_with_limit_without_offset(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 3
    assert len(ds.df) == 5
    assert ds.pagination_info.pagination_info.type == "unknown_size"


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_none),
)
def test_get_slice_with_limit_without_offset_no_data(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 3
    assert len(ds.df) == 0
    assert ds.pagination_info.pagination_info.total_rows == 0


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_one),
)
def test_get_slice_with_limit_without_offset_not_enough_data(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, limit=5)
    assert result.call_count == 3
    assert len(ds.df) == 1
    assert ds.pagination_info.pagination_info.total_rows == 1


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_5),
)
def test_get_slice_with_limit_with_offset(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 3
    assert len(ds.df) == 5
    assert ds.pagination_info.pagination_info.type == "unknown_size"


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_none),
)
def test_get_slice_with_limit_with_offset_no_data(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 3
    assert len(ds.df) == 0
    assert ds.pagination_info.pagination_info.total_rows == 5
    assert ds.pagination_info.next_page is None
    assert ds.pagination_info.previous_page == OffsetLimitInfo(offset=0, limit=5)


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_one),
)
def test_get_slice_with_limit_with_offset_not_enough_data(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5, limit=5)
    assert result.call_count == 3
    assert len(ds.df) == 1
    # offset of 5 + 1 actually retrieved row
    assert ds.pagination_info.pagination_info.total_rows == 6


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
@patch("snowflake.connector.cursor.SnowflakeCursor.execute")
@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_without_limit_with_offset(result, execute_query, connect, snowflake_datasource):
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource, offset=5)
    assert result.call_count == 3
    assert len(ds.df) == 14
    # Offset of 5 + 14 actually retrieved rows
    assert ds.pagination_info.pagination_info.total_rows == 19


@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_metadata(snowflake_datasource, mocker):
    snowflake_datasource.query = "select name from favourite_drinks limit 12 offset 23;"
    connect = mocker.MagicMock()
    connect.cursor().execute().fetchone.return_value = [{"total_rows": 200}]
    connect.cursor().execute().fetchall.return_value = [{"c1": 2}]
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)

    # testing this is stupid
    expected_memory_size = 1364
    assert ds.stats.df_memory_size == expected_memory_size
    assert ds.pagination_info.pagination_info.total_rows == 14


@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_metadata_no_select_in_query(result, snowflake_datasource, mocker):
    snowflake_datasource.query = (
        "create table users as  (id integer default id_seq.nextval,  name varchar (100), "
        "preferences string, created_at timestamp); "
    )
    connect = mocker.MagicMock()
    ds: DataSlice = SnowflakeCommon().get_slice(connect, snowflake_datasource)
    assert result.call_count == 3
    assert ds


@patch(
    "pandas.DataFrame.from_dict",
    return_value=pd.DataFrame(data_result_all),
)
def test_get_slice_metadata_wrong_response_from_count_query(snowflake_datasource, mocker):
    snowflake_datasource.query = "select name from favourite_drinks limit 12 offset 23;"
    connect = mocker.MagicMock()
    connect.cursor().execute().fetchone.return_value = [{"error": "invalid query"}]
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
    "toucan_connectors.snowflake_common.SnowflakeCommon._execute_query",
    side_effect=ProgrammingError,
)
def test_execute_broken_query(execute_query, snowflake_datasource, mocker):
    snowflake_datasource.query = "select name from favourite_drinks limit 12 offset 23;"
    connect = mocker.MagicMock()
    with pytest.raises(ProgrammingError):
        SnowflakeCommon()._execute_parallelized_queries(
            connect,
            query=snowflake_datasource.query,
            query_parameters=snowflake_datasource.parameters,
        )


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
def test_describe(connect, mocker):
    mocked__describe = mocker.patch("toucan_connectors.query_manager.QueryManager.describe")
    SnowflakeCommon().describe(connect, "SELECT FAIRY_DUST FROM STRATON_OAKMONT;")
    mocked__describe.assert_called_once()


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
def test__describe(connect, mocker):
    mocked_cursor = mocker.MagicMock()
    mocked_describe = mocked_cursor.describe
    mocker.patch("toucan_connectors.snowflake_common.json.dumps")

    class fake_result:
        def __init__(self, name, type_code):
            self.name = name
            self.type_code = type_code

    mocked_describe.return_value = [
        fake_result(name="steve_madden", type_code=0),
        fake_result(name="IPO", type_code=0),
    ]
    connect.cursor.return_value = mocked_cursor
    res = SnowflakeCommon()._describe(connect, "SELECT steve_madden, IPO FROM STRATON_OAKMONT;")
    mocked_describe.assert_called_once()
    assert res["steve_madden"] == "float"
    assert res["IPO"] == "float"


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
def test__describe_api_changed(connect, mocker):
    mocked_cursor = mocker.MagicMock()
    mocked_describe = mocked_cursor.describe
    mocker.patch("toucan_connectors.snowflake_common.json.dumps")

    class fake_result:
        def __init__(self, name, type_code):
            self.name = name
            self.type_code = type_code

    mocked_describe.return_value = [
        fake_result(name="steve_madden", type_code=14),
        fake_result(name="IPO", type_code=0),
    ]
    connect.cursor.return_value = mocked_cursor
    res = SnowflakeCommon()._describe(connect, "SELECT steve_madden, IPO FROM STRATON_OAKMONT;")
    mocked_describe.assert_called_once()
    assert res["steve_madden"] is None
    assert res["IPO"] == "float"


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
def test__describe_api_didnt_describe(connect, mocker):
    mocked_cursor = mocker.MagicMock()
    mocked_describe = mocked_cursor.describe
    mocked_describe.return_value = None
    connect.cursor.return_value = mocked_cursor
    with pytest.raises(TypeError):
        SnowflakeCommon()._describe(connect, "SELECT steve_madden, IPO FROM STRATON_OAKMONT;")
    mocked_describe.assert_called_once()


@patch(
    "pandas.DataFrame.from_dict",
    side_effect=[
        None,
        None,
        pd.DataFrame(data_result_all),
        pd.DataFrame({"TOTAL_ROWS": 20}, index=[0]),
    ],
)
@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
def test_retrieve_data_with_row_count_limit_in_query(connect, fetchmany, snowflake_datasource):
    snowflake_datasource.query = "select name from favourite_drinks limit 10;"
    sc = SnowflakeCommon()
    sc.retrieve_data(connect, snowflake_datasource, get_row_count=True)
    assert fetchmany.call_count == 4  # +1 to select database and warehouse
    assert sc.total_rows_count == 20


def test_retrieve_total_rows():
    sc = SnowflakeCommon()
    sc.set_total_returned_rows_count(20)
    assert sc.total_returned_rows_count == 20


@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon._execute_query",
)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon._execute_parallelized_queries",
)
@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
def test_fetch_data_warehouse_none(execute_query, execute_parallelized, connect):
    """The connection's warehouse should not be switched to datasource's if none"""
    s = SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_1",
        warehouse=None,
        query="select * from my_table where toto=%(foo);",
        query_object={"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]},
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )
    SnowflakeCommon().fetch_data(connect, s)
    assert execute_query.call_count == 0


@patch("snowflake.connector.connect", return_value=snowflake.connector.SnowflakeConnection)
def test_get_db_content(connect, mocker):
    scommon = SnowflakeCommon()
    mocker.patch.object(
        scommon,
        "_execute_query",
        return_value={
            "DATABASE": "SNOWFLAKE_SAMPLE_DATA",
            "SCHEMA": "TPCH_SF1000",
            "TYPE": "table",
            "NAME": "REGION",
            "COLUMNS": '[\n  {\n    "name": "R_COMMENT",\n    "type": "TEXT"\n  },\n  {\n    "name": '
            '"R_COMMENT",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_NAME",\n    "type": '
            '"TEXT"\n  },\n  {\n    "name": "R_REGIONKEY",\n    "type": "NUMBER"\n  },\n  {\n    '
            '"name": "R_REGIONKEY",\n    "type": "NUMBER"\n  },\n  {\n    "name": "R_NAME",'
            '\n    "type": "TEXT"\n  },\n  {\n    "name": "R_COMMENT",\n    "type": "TEXT"\n  },'
            '\n  {\n    "name": "R_NAME",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_NAME",'
            '\n    "type": "TEXT"\n  },\n  {\n    "name": "R_REGIONKEY",\n    "type": "NUMBER"\n  '
            '},\n  {\n    "name": "R_COMMENT",\n    "type": "TEXT"\n  },\n  {\n    "name": '
            '"R_REGIONKEY",\n    "type": "NUMBER"\n  }\n]',
        },
    )
    assert scommon.get_db_content(connection=connect) == {
        "DATABASE": "SNOWFLAKE_SAMPLE_DATA",
        "SCHEMA": "TPCH_SF1000",
        "TYPE": "table",
        "NAME": "REGION",
        "COLUMNS": '[\n  {\n    "name": "R_COMMENT",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_COMMENT",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_NAME",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_REGIONKEY",\n    "type": "NUMBER"\n  },\n  {\n    "name": "R_REGIONKEY",\n    "type": "NUMBER"\n  },\n  {\n    "name": "R_NAME",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_COMMENT",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_NAME",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_NAME",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_REGIONKEY",\n    "type": "NUMBER"\n  },\n  {\n    "name": "R_COMMENT",\n    "type": "TEXT"\n  },\n  {\n    "name": "R_REGIONKEY",\n    "type": "NUMBER"\n  }\n]',  # noqa: E501
    }
