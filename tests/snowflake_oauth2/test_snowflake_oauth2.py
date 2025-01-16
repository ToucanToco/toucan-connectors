import time
from unittest.mock import patch

import pandas as pd
import pytest
from pandas import DataFrame
from snowflake.connector import SnowflakeConnection

from toucan_connectors import DataSlice
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector, SecretsKeeper
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
        name="snowflake",
        secrets_keeper=LocalSecretsKeeper(),
        auth_flow_id="snowflake",
        authorization_url="AUTHORIZATION_URL",
        redirect_uri="REDIRECT_URI",
        scope="refresh_token",
        client_id="CLIENT_ID",
        client_secret="CLIENT_SECRET",
        role="BENCHMARK_ANALYST",
        default_warehouse="warehouse_1",
        account="toucantocopartner.west-europe.azure",
        identifier="small_app_test" + "_" + "snowflake",
    )


@pytest.fixture
def snowflake_oauth2_datasource():
    return SnowflakeoAuth2DataSource(
        name="test_name",
        domain="test_domain",
        database="database_1",
        warehouse="warehouse_1",
        query="test_query with %(foo)s and %(pokemon)s",
        query_object={"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]},
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )


data = JsonWrapper.load(
    open(
        "tests/snowflake_oauth2/fixture/data.json",
    )
)
df = pd.DataFrame(
    data,
    columns=[
        "1 Column Name",
        "2 Column Name",
        "3 Column Name",
        "4 Column Name",
        "5 Column Name",
        "6 Column Name",
        "7 Column Name",
        "8 Column Name",
        "9 Column Name",
        "10 Column Name",
    ],
)


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_databases",
    return_value=["database_1", "database_2"],
)
def test_get_database_without_filter(gd, is_closed, close, connect, get_token, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_databases()
    assert result[0] == "database_1"
    assert result[1] == "database_2"
    assert len(result) == 2
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon.get_databases", return_value=["database_1"])
def test_get_database_with_filter_found(gd, is_closed, close, connect, get_token, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_databases("database_1")
    assert result[0] == "database_1"
    assert len(result) == 1
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon.get_databases", return_value=[])
def test_get_database_with_filter_not_found(gd, is_closed, close, connect, get_token, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_databases("database_3")
    assert len(result) == 0
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1", "warehouse_2"],
)
def test_get_warehouse_without_filter(gw, is_closed, close, connect, get_token, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_warehouses()
    assert result[0] == "warehouse_1"
    assert result[1] == "warehouse_2"
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
def test_get_warehouse_with_filter_found(gw, is_closed, close, connect, get_token, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_warehouses("warehouse_1")
    assert result[0] == "warehouse_1"
    assert len(result) == 1
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses", return_value=[])
def test_get_warehouse_with_filter_not_found(gw, is_closed, close, connect, get_token, snowflake_oauth2_connector):
    result = snowflake_oauth2_connector._get_warehouses("warehouse_3")
    assert len(result) == 0
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data(
    eq,
    is_closed,
    close,
    connect,
    get_token,
    snowflake_oauth2_connector,
    snowflake_oauth2_datasource,
):
    df_result: DataFrame = snowflake_oauth2_connector._retrieve_data(snowflake_oauth2_datasource)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result)
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_slice(
    eq,
    is_closed,
    close,
    connect,
    get_token,
    snowflake_oauth2_connector,
    snowflake_oauth2_datasource,
):
    df_result: DataSlice = snowflake_oauth2_connector.get_slice(snowflake_oauth2_datasource, offset=0, limit=10)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result.df)
    assert df_result.pagination_info.pagination_info.type == "unknown_size"
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_slice_with_limit(
    eq,
    is_closed,
    close,
    connect,
    get_token,
    snowflake_oauth2_connector,
    snowflake_oauth2_datasource,
):
    df_result: DataSlice = snowflake_oauth2_connector.get_slice(snowflake_oauth2_datasource, offset=5, limit=3)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result.df)
    assert df_result.pagination_info.pagination_info.type == "unknown_size"
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_slice_too_much(
    eq,
    is_closed,
    close,
    connect,
    get_token,
    snowflake_oauth2_connector,
    snowflake_oauth2_datasource,
):
    df_result: DataSlice = snowflake_oauth2_connector.get_slice(snowflake_oauth2_datasource, offset=10, limit=20)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result.df)
    assert 21 == df_result.pagination_info.pagination_info.total_rows
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1", "warehouse_2"],
)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_databases",
    return_value=["database_1", "database_2"],
)
def test_datasource_get_form(
    gd,
    gw,
    is_closed,
    close,
    connect,
    get_token,
    snowflake_oauth2_connector,
    snowflake_oauth2_datasource,
):
    result = SnowflakeoAuth2DataSource.get_form(snowflake_oauth2_connector, {})
    assert "warehouse_1" == result["properties"]["warehouse"]["default"]
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_databases",
    return_value=["database_1", "database_2"],
)
def test_datasource_get_databases(
    gd,
    is_closed,
    close,
    connect,
    get_token,
    snowflake_oauth2_connector,
    snowflake_oauth2_datasource,
):
    result = SnowflakeoAuth2DataSource._get_databases(snowflake_oauth2_connector)
    assert len(result) == 2
    assert result[1] == "database_2"
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_snowflake_connection_success(is_closed, close, connect, get_token, snowflake_oauth2_connector):
    snowflake_oauth2_connector._get_connection("test_database", "test_warehouse")
    assert connect.call_count == 1
    assert connect.call_args_list[0][1]["account"] == "toucantocopartner.west-europe.azure"
    assert connect.call_args_list[0][1]["role"] == "BENCHMARK_ANALYST"
    assert connect.call_args_list[0][1]["token"] == "tortank"
    assert connect.call_args_list[0][1]["database"] == "test_database"
    assert connect.call_args_list[0][1]["warehouse"] == "test_warehouse"
    SnowflakeoAuth2Connector.get_connection_manager().force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("snowflake.connector.connection.SnowflakeConnection.close", return_value=None)
@patch("snowflake.connector.connection.SnowflakeConnection.is_closed", return_value=None)
def test_snowflake_connection_alive(is_closed, close, connect, get_token, snowflake_oauth2_connector):
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    snowflake_oauth2_connector._get_connection("test_database", "test_warehouse")
    time.sleep(4)
    assert is_closed.call_count >= 1
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("snowflake.connector.connection.SnowflakeConnection.close", return_value=None)
@patch(
    "snowflake.connector.connection.SnowflakeConnection.is_closed",
    side_effect=TypeError("is_closed is not a function"),
)
def test_snowflake_connection_alive_exception(is_closed, close, connect, get_token, snowflake_oauth2_connector):
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    snowflake_oauth2_connector._get_connection("test_database", "test_warehouse")
    time.sleep(4)
    assert is_closed.call_count >= 1
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("snowflake.connector.connection.SnowflakeConnection.close", return_value=None)
@patch("snowflake.connector.connection.SnowflakeConnection.is_closed", return_value=None)
def test_snowflake_connection_close(is_closed, close, connect, get_token, snowflake_oauth2_connector):
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    snowflake_oauth2_connector._get_connection("test_database", "test_warehouse")
    assert len(cm.connection_list) == 1
    time.sleep(4)
    assert close.call_count == 1
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch(
    "snowflake.connector.connection.SnowflakeConnection.close",
    side_effect=TypeError("close is not a function"),
)
@patch("snowflake.connector.connection.SnowflakeConnection.is_closed", return_value=True)
def test_snowflake_connection_close_exception(is_closed, close, connect, get_token, snowflake_oauth2_connector):
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    with snowflake_oauth2_connector._get_connection("test_database", "test_warehouse"):
        assert len(cm.connection_list) == 1
    time.sleep(1)
    assert close.call_count == 1
    assert len(cm.connection_list) == 1
    time.sleep(5)
    assert len(cm.connection_list) == 0
    assert close.call_count == 3
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


def test_build_authorization_url(mocker, snowflake_oauth2_connector):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "test_client_id"
    mock_oauth2_connector.client_secret = "test_client_secret"
    snowflake_oauth2_connector._oauth2_connector = mock_oauth2_connector
    snowflake_oauth2_connector.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_tokens(mocker, snowflake_oauth2_connector):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "test_client_id"
    mock_oauth2_connector.client_secret = "test_client_secret"
    snowflake_oauth2_connector._oauth2_connector = mock_oauth2_connector
    snowflake_oauth2_connector.retrieve_tokens("bla")
    mock_oauth2_connector.retrieve_tokens.assert_called()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_describe(
    is_closed,
    close,
    connect,
    get_token,
    mocker,
    snowflake_oauth2_datasource,
    snowflake_oauth2_connector,
):
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    mocked_common_describe = mocker.patch(
        "toucan_connectors.snowflake_common.SnowflakeCommon.describe",
        return_value={"toto": "int", "tata": "str"},
    )
    snowflake_oauth2_connector.describe(snowflake_oauth2_datasource)
    mocked_common_describe.assert_called_once()
    cm.force_clean()


def test_set_warehouse(snowflake_oauth2_connector, snowflake_oauth2_datasource):
    snowflake_oauth2_datasource.warehouse = None
    new_data_source = snowflake_oauth2_connector._set_warehouse(snowflake_oauth2_datasource)
    assert new_data_source.warehouse == "warehouse_1"


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_get_model(
    is_closed,
    close,
    connect,
    get_token,
    mocker,
    snowflake_oauth2_datasource,
    snowflake_oauth2_connector,
):
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    mocked_common_get_databases = mocker.patch(
        "toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeCommon.get_databases",
        return_value=["booo"],
    )
    mocked_common_get_db_content = mocker.patch(
        "toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeCommon.get_db_content",
        return_value=pd.DataFrame(
            [
                {
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
                }
            ]
        ),
    )
    res = snowflake_oauth2_connector.get_model()
    mocked_common_get_databases.assert_called_once()
    mocked_common_get_db_content.assert_called_once()
    assert res == [
        {
            "name": "REGION",
            "schema": "TPCH_SF1000",
            "database": "SNOWFLAKE_SAMPLE_DATA",
            "type": "table",
            "columns": [
                {"name": "R_COMMENT", "type": "TEXT"},
                {"name": "R_COMMENT", "type": "TEXT"},
                {"name": "R_NAME", "type": "TEXT"},
                {"name": "R_REGIONKEY", "type": "NUMBER"},
                {"name": "R_REGIONKEY", "type": "NUMBER"},
                {"name": "R_NAME", "type": "TEXT"},
                {"name": "R_COMMENT", "type": "TEXT"},
                {"name": "R_NAME", "type": "TEXT"},
                {"name": "R_NAME", "type": "TEXT"},
                {"name": "R_REGIONKEY", "type": "NUMBER"},
                {"name": "R_COMMENT", "type": "TEXT"},
                {"name": "R_REGIONKEY", "type": "NUMBER"},
            ],
        }
    ]
    cm.force_clean()


@patch(
    "toucan_connectors.oauth2_connector.oauth2connector.OAuth2Connector.get_access_token",
    return_value="tortank",
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_get_model_exception(
    is_closed,
    close,
    connect,
    get_token,
    mocker,
    snowflake_oauth2_datasource,
    snowflake_oauth2_connector,
):
    cm = SnowflakeoAuth2Connector.get_connection_manager()
    mocked_common_get_databases = mocker.patch(
        "toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeCommon.get_databases",
        return_value=["booo"],
    )
    mocker.patch(
        "toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector.SnowflakeCommon.get_db_content",
        side_effect=Exception,
    )

    with pytest.raises(Exception):
        snowflake_oauth2_connector.get_model()
    mocked_common_get_databases.assert_called_once()
    cm.force_clean()
