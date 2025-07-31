# flake8: noqa: Q000
import time
from datetime import datetime, timedelta
from os import environ
from pathlib import Path
from urllib.error import HTTPError

import jwt
import pandas as pd
import pytest
import snowflake
from mock import patch
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pydantic import SecretStr
from snowflake.connector import SnowflakeConnection

from toucan_connectors import DataSlice
from toucan_connectors.common import ConnectorStatus
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.snowflake import (
    AuthenticationMethod,
    SnowflakeConnector,
    SnowflakeDataSource,
)

OAUTH_TOKEN_ENDPOINT = "http://example.com/endpoint"
OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE = "application/x-www-form-urlencoded"
OAUTH_ACCESS_TOKEN = str(jwt.encode({"exp": 42, "sub": "snowflake_user"}, key="clef"))
OAUTH_REFRESH_TOKEN = "baba au rhum"
OAUTH_CLIENT_ID = "client_id"
OAUTH_CLIENT_SECRET = "client_s3cr3t"


@pytest.fixture
def snowflake_connector_oauth(mocker):
    user_tokens_keeper = mocker.Mock(
        access_token=SecretStr(OAUTH_ACCESS_TOKEN),
        refresh_token=SecretStr(OAUTH_REFRESH_TOKEN),
        update_tokens=mocker.Mock(),
    )
    sso_credentials_keeper = mocker.Mock(
        client_id=OAUTH_CLIENT_ID, client_secret=SecretStr(OAUTH_CLIENT_SECRET)
    )
    return SnowflakeConnector(
        name="test_name",
        authentication_method=AuthenticationMethod.OAUTH,
        user="test_user",
        password="test_password",
        account="test_account",
        token_endpoint=OAUTH_TOKEN_ENDPOINT,
        token_endpoint_content_type=OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE,
        user_tokens_keeper=user_tokens_keeper,
        sso_credentials_keeper=sso_credentials_keeper,
        default_warehouse="default_wh",
    )


@pytest.fixture
def snowflake_connector():
    return SnowflakeConnector(
        identifier="snowflake_test",
        name="test_name",
        authentication_method=AuthenticationMethod.PLAIN,
        user="test_user",
        password="test_password",
        account="test_account",
        default_warehouse="warehouse_1",
    )


@pytest.fixture
def snowflake_connector_malformed():
    return SnowflakeConnector(
        identifier="snowflake_test",
        name="test_name",
        user="test_user",
        password="test_password",
        account="test_account",
        default_warehouse="warehouse_1",
    )


@pytest.fixture
def snowflake_datasource():
    return SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_1",
        warehouse="warehouse_1",
        query="test_query with %(foo)s and %(pokemon)s",
        query_object={
            "schema": "SHOW_SCHEMA",
            "table": "MY_TABLE",
            "columns": ["col1", "col2"],
        },
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )


data = JsonWrapper.load(
    open(
        "tests/snowflake/fixture/data.json",
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


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_databases",
    return_value=["database_1", "database_2"],
)
def test_datasource_get_databases(
    gd, is_closed, close, connect, snowflake_connector, snowflake_datasource
):
    result = snowflake_datasource._get_databases(snowflake_connector)
    assert len(result) == 2
    assert result[1] == "database_2"
    assert snowflake_datasource.language == "sql"
    assert snowflake_datasource.query_object == {
        "schema": "SHOW_SCHEMA",
        "table": "MY_TABLE",
        "columns": ["col1", "col2"],
    }
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


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
    gd, gw, is_closed, close, connect, snowflake_connector, snowflake_datasource
):
    result = snowflake_datasource.get_form(snowflake_connector, {})
    assert "warehouse_1" == result["properties"]["warehouse"]["default"]
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


def test_set_warehouse(snowflake_connector, snowflake_datasource):
    snowflake_datasource.warehouse = None
    new_data_source = snowflake_connector._set_warehouse(snowflake_datasource)
    assert new_data_source.warehouse == "warehouse_1"
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


# TODO - Que fait on lorsqu'il n'y a pas de default warehouse ? Les requêtes sont quand même exécutées ?
def test_set_warehouse_without_default_warehouse(snowflake_datasource):
    sc_without_default_warehouse = SnowflakeConnector(
        identifier="snowflake_test",
        name="test_name",
        authentication_method=AuthenticationMethod.PLAIN,
        user="test_user",
        password="test_password",
        account="test_account",
    )
    snowflake_datasource.warehouse = None
    new_data_source = sc_without_default_warehouse._set_warehouse(snowflake_datasource)
    assert new_data_source.warehouse is None
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_databases",
    return_value=["database_1", "database_2"],
)
def test_get_database_without_filter(gd, is_closed, close, connect, snowflake_connector):
    result = snowflake_connector._get_databases()
    assert result[0] == "database_1"
    assert result[1] == "database_2"
    assert len(result) == 2
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_databases",
    return_value=["database_1"],
)
def test_get_database_with_filter_found(gd, is_closed, close, connect, snowflake_connector):
    result = snowflake_connector._get_databases("database_1")
    assert result[0] == "database_1"
    assert len(result) == 1
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon.get_databases", return_value=[])
def test_get_database_with_filter_not_found(gd, is_closed, close, connect, snowflake_connector):
    result = snowflake_connector._get_databases("database_3")
    assert len(result) == 0
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1", "warehouse_2"],
)
def test_get_warehouse_without_filter(gw, is_closed, close, connect, snowflake_connector):
    result = snowflake_connector._get_warehouses()
    assert result[0] == "warehouse_1"
    assert result[1] == "warehouse_2"
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
def test_get_warehouse_with_filter_found(gw, is_closed, close, connect, snowflake_connector):
    result = snowflake_connector._get_warehouses("warehouse_1")
    assert result[0] == "warehouse_1"
    assert len(result) == 1
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses", return_value=[])
def test_get_warehouse_with_filter_not_found(gw, is_closed, close, connect, snowflake_connector):
    result = snowflake_connector._get_warehouses("warehouse_3")
    assert len(result) == 0
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data(
    eq, is_closed, close, connect, snowflake_connector, snowflake_datasource, mocker
):
    df_result: DataFrame = snowflake_connector._retrieve_data(snowflake_datasource)

    assert eq.call_count == 3  # +1 for each data request
    assert 11 == len(df_result)
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_slice(
    eq, is_closed, close, connect, snowflake_connector, snowflake_datasource, mocker
):
    df_result: DataSlice = snowflake_connector.get_slice(snowflake_datasource)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result.df)
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_slice_offset_limit(
    eq, is_closed, close, connect, snowflake_connector, snowflake_datasource, mocker
):
    df_result: DataSlice = snowflake_connector.get_slice(snowflake_datasource, offset=5, limit=3)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result.df)
    assert 11 == df_result.stats.total_returned_rows
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_slice_too_much(
    eq, is_closed, close, connect, snowflake_connector, snowflake_datasource, mocker
):
    df_result: DataSlice = snowflake_connector.get_slice(snowflake_datasource, offset=10, limit=20)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result.df)
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_fetch(
    eq, is_closed, close, connect, snowflake_connector, snowflake_datasource, mocker
):
    df_result = snowflake_connector._fetch_data(snowflake_datasource)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result)
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_fetch_offset_limit(
    eq, is_closed, close, connect, snowflake_connector, snowflake_datasource, mocker
):
    df_result: DataSlice = snowflake_connector._fetch_data(snowflake_datasource, offset=5, limit=3)
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result)
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon._execute_query", return_value=df)
def test_retrieve_data_fetch_too_much(
    eq, is_closed, close, connect, snowflake_connector, snowflake_datasource, mocker
):
    df_result: DataSlice = snowflake_connector._fetch_data(
        snowflake_datasource, offset=10, limit=20
    )
    assert eq.call_count == 3  # +1 select database and warehouse
    assert 11 == len(df_result)
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


def test_schema_fields_order():
    schema_props_keys = list(
        JsonWrapper.loads(SnowflakeConnector.schema_json())["properties"].keys()
    )
    ordered_keys = [
        "type",
        "name",
        "account",
        "authentication_method",
        "user",
        "password",
        "private_key",
        "token_endpoint",
        "token_endpoint_content_type",
        "role",
        "default_warehouse",
        "retry_policy",
        "secrets_storage_version",
        "sso_credentials_keeper",
        "user_tokens_keeper",
    ]
    assert schema_props_keys == ordered_keys


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
def test_get_status_all_good(gw, is_closed, close, connect, snowflake_connector):
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=True,
        details=[("Connection to Snowflake", True), ("Default warehouse exists", True)],
    )
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
def test_get_status(gw, is_closed, close, connect, snowflake_connector):
    connector_status = snowflake_connector.get_status()
    assert connector_status.status
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses", return_value=[])
def test_get_status_without_warehouses(gw, is_closed, close, connect, snowflake_connector):
    connector_status = snowflake_connector.get_status()
    assert not connector_status.status
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()


@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_get_status_account_nok(is_closed, close, connect, gw, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    gw.side_effect = snowflake.connector.errors.ProgrammingError("Account nok")
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error="Account nok",
        details=[
            ("Connection to Snowflake", False),
            ("Default warehouse exists", None),
        ],
    )
    cm.force_clean()


@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_account_does_not_exists(is_closed, close, connect, gw, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    gw.side_effect = snowflake.connector.errors.OperationalError()
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error=f"Connection failed for the account '{snowflake_connector.account}', please check the Account field",
        details=[
            ("Connection to Snowflake", False),
            ("Default warehouse exists", None),
        ],
    )
    cm.force_clean()


@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_account_forbidden(is_closed, close, connect, gw, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    gw.side_effect = snowflake.connector.errors.ForbiddenError()
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error=f"Access forbidden, please check that you have access to the '{snowflake_connector.account}' account or try again later.",
        details=[
            ("Connection to Snowflake", False),
            ("Default warehouse exists", None),
        ],
    )
    cm.force_clean()


@patch(
    "toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses",
    return_value=["warehouse_1"],
)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_account_failed_for_user(is_closed, close, connect, gw, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    gw.side_effect = snowflake.connector.errors.DatabaseError()
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error=f"Connection failed for the user '{snowflake_connector.user}', please check your credentials",
        details=[
            ("Connection to Snowflake", False),
            ("Default warehouse exists", None),
        ],
    )
    cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._refresh_oauth_token")
def test_get_connection_connect(rt, is_closed, close, connect, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    snowflake_connector._get_connection("test_database", "test_warehouse")
    assert rt.call_count == 0
    assert connect.call_args_list[0][1]["account"] == "test_account"
    assert connect.call_args_list[0][1]["user"] == "test_user"
    assert connect.call_args_list[0][1]["password"] == "test_password"
    assert connect.call_args_list[0][1]["database"] == "test_database"
    assert connect.call_args_list[0][1]["warehouse"] == "test_warehouse"
    cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("snowflake.connector.connection.SnowflakeConnection.close", return_value=None)
@patch("snowflake.connector.connection.SnowflakeConnection.is_closed", return_value=None)
@patch("toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._refresh_oauth_token")
def test_snowflake_connection_alive(gat, is_closed, close, connect, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 5
    snowflake_connector._get_connection("test_database", "test_warehouse")
    assert len(cm.connection_list) == 1
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("snowflake.connector.connection.SnowflakeConnection.close", return_value=None)
@patch(
    "snowflake.connector.connection.SnowflakeConnection.is_closed",
    side_effect=TypeError("is_closed is not a function"),
)
@patch("toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._refresh_oauth_token")
def test_snowflake_connection_alive_exception(gat, is_closed, close, connect, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    snowflake_connector._get_connection("test_database", "test_warehouse")
    assert len(cm.connection_list) == 1
    time.sleep(4)
    assert is_closed.call_count >= 1
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("snowflake.connector.connection.SnowflakeConnection.close", return_value=None)
@patch("snowflake.connector.connection.SnowflakeConnection.is_closed", return_value=None)
@patch("toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._refresh_oauth_token")
def test_snowflake_connection_close(gat, is_closed, close, connect, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    snowflake_connector._get_connection("test_database", "test_warehouse")
    time.sleep(5)
    assert close.call_count >= 1
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch(
    "snowflake.connector.connection.SnowflakeConnection.close",
    side_effect=TypeError("close is not a function"),
)
@patch("snowflake.connector.connection.SnowflakeConnection.is_closed", return_value=True)
@patch("toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._refresh_oauth_token")
def test_snowflake_connection_close_exception(gat, is_closed, close, connect, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    t1 = cm.time_between_clean
    t2 = cm.time_keep_alive
    cm.time_between_clean = 1
    cm.time_keep_alive = 1
    snowflake_connector._get_connection("test_database", "test_warehouse")
    time.sleep(2)
    assert close.call_count >= 1
    time.sleep(5)
    assert len(cm.connection_list) == 0
    assert close.call_count >= 1
    cm.time_between_clean = t1
    cm.time_keep_alive = t2
    cm.force_clean()


@patch("toucan_connectors.snowflake_common.SnowflakeCommon.retrieve_data", return_value=df)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("snowflake.connector.connection.SnowflakeConnection.close", return_value=None)
@patch("snowflake.connector.connection.SnowflakeConnection.is_closed", return_value=None)
@patch("toucan_connectors.ToucanConnector.get_identifier", return_value="test")
def test_oauth_args_wrong_type_of_auth(
    get_identifier,
    is_closed,
    close,
    connect,
    retrieve_data,
    snowflake_connector_oauth,
    snowflake_datasource,
    mocker,
):
    spy = mocker.spy(SnowflakeConnector, "_refresh_oauth_token")

    snowflake_connector_oauth.authentication_method = AuthenticationMethod.PLAIN
    snowflake_connector_oauth._retrieve_data(snowflake_datasource)
    SnowflakeConnector.get_snowflake_connection_manager().force_clean()
    assert spy.call_count == 0


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.ToucanConnector.get_identifier", return_value="test")
@patch("requests.post")
def test_oauth_args_endpoint_not_200(
    req_mock, is_closed, close, connect, snowflake_connector_oauth, snowflake_datasource
):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    snowflake_connector_oauth.user_tokens_keeper.access_token = SecretStr(
        jwt.encode({"exp": datetime.now() - timedelta(hours=24)}, key="supersecret")
    )
    req_mock.return_value.status_code = 401

    def fake_raise_for_status():
        raise HTTPError("url", 401, "Unauthorized", {}, None)

    req_mock.return_value.ok = False
    req_mock.return_value.raise_for_status = lambda: fake_raise_for_status()

    try:
        snowflake_connector_oauth._retrieve_data(snowflake_datasource)
    except Exception as e:
        cm.force_clean()
        assert str(e) == "HTTP Error 401: Unauthorized"
        assert req_mock.call_count == 1
    else:
        cm.force_clean()


@patch("toucan_connectors.snowflake_common.SnowflakeCommon.retrieve_data", return_value=df)
@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.ToucanConnector.get_identifier", return_value="test")
@patch("requests.post")
def test_refresh_oauth_token(
    req_mock,
    get_identifier,
    is_closed,
    close,
    connect,
    retrieve_data,
    snowflake_connector_oauth,
    snowflake_datasource,
):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    # Expired JWT
    snowflake_connector_oauth.user_tokens_keeper.access_token = SecretStr(
        jwt.encode({"exp": datetime.now() - timedelta(hours=24)}, key="supersecret")
    )
    req_mock.return_value.status_code = 201
    req_mock.return_value.ok = False
    req_mock.return_value.return_value = {
        "access_token": "token",
        "refresh_token": "token",
    }

    try:
        snowflake_connector_oauth._retrieve_data(snowflake_datasource)
        assert req_mock.call_count == 1
    except Exception as e:
        assert str(e) == "HTTP Error 401: Unauthorized"
        assert False
    else:
        assert True
    finally:
        cm.force_clean()

    req_mock.reset_mock()
    # Invalid JWT
    snowflake_connector_oauth.user_tokens_keeper.access_token = SecretStr("PLOP")
    try:
        snowflake_connector_oauth._retrieve_data(snowflake_datasource)
        assert req_mock.call_count == 1
    except Exception as e:
        assert str(e) == "HTTP Error 401: Unauthorized"
        assert False
    else:
        assert True
    finally:
        cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
@patch("toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._refresh_oauth_token")
@patch("toucan_connectors.ToucanConnector.get_identifier", return_value="test")
def test_get_connection_connect_oauth(
    get_identifier, rt, is_closed, close, connect, snowflake_connector_oauth
):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    snowflake_connector_oauth._get_connection("test_database", "test_warehouse")
    print(connect.call_args_list)
    assert rt.call_count == 1
    assert connect.call_args_list[0][1]["account"] == "test_account"
    assert (
        connect.call_args_list[0][1]["token"]
        == "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjQyLCJzdWIiOiJzbm93Zmxha2VfdXNlciJ9.NJDbR-tAepC_ANrg9m5PozycbcuWDgGi4o9sN9Pl27k"
    )
    assert connect.call_args_list[0][1]["database"] == "test_database"
    assert connect.call_args_list[0][1]["warehouse"] == "test_warehouse"
    cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_describe(is_closed, close, connect, mocker, snowflake_datasource, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    mocked_common_describe = mocker.patch(
        "toucan_connectors.snowflake.snowflake_connector.SnowflakeCommon.describe",
        return_value={"toto": "int", "tata": "str"},
    )
    snowflake_connector.describe(snowflake_datasource)
    mocked_common_describe.assert_called_once()
    cm.force_clean()


def test_get_unique_datasource_identifier():
    snowflake_connector = SnowflakeConnector(
        identifier="snowflake_test",
        name="test_name",
        authentication_method=AuthenticationMethod.PLAIN,
        user="test_user",
        password="test_password",
        account="test_account",
        default_warehouse="warehouse_1",
    )

    datasource = SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_1",
        warehouse="warehouse_1",
        query="test_query with %(foo)s and %(pokemon)s",
        query_object={
            "schema": "SHOW_SCHEMA",
            "table": "MY_TABLE",
            "columns": ["col1", "col2"],
        },
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )
    key = snowflake_connector.get_cache_key(datasource)

    datasource2 = SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_1",
        warehouse="warehouse_1",
        query="test_query with %(foo)s and %(pokemon)s",
        query_object={
            "schema": "SHOW_SCHEMA",
            "table": "MY_TABLE",
            "columns": ["col1", "col2"],
        },
        parameters={"foo": "bar", "pokemon": "pikachu", "foo": "bar"},
    )
    key2 = snowflake_connector.get_cache_key(datasource2)

    assert key == key2

    datasource3 = SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_2",
        warehouse="warehouse_1",
        query="test_query with %(foo)s and %(pokemon)s",
        query_object={
            "schema": "SHOW_SCHEMA",
            "table": "MY_TABLE",
            "columns": ["col1", "col2"],
        },
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )

    key3 = snowflake_connector.get_cache_key(datasource3)
    assert key != key3

    another_snowflake_connector = SnowflakeConnector(
        identifier="snowflake_test",
        name="test_name",
        authentication_method=AuthenticationMethod.PLAIN,
        user="test_user",
        password="test_password",
        account="another_test_account",
        default_warehouse="warehouse_1",
    )

    assert snowflake_connector.get_cache_key(
        datasource
    ) != another_snowflake_connector.get_cache_key(datasource)
    assert snowflake_connector.get_cache_key(
        datasource2
    ) != another_snowflake_connector.get_cache_key(datasource2)
    assert snowflake_connector.get_cache_key(
        datasource3
    ) != another_snowflake_connector.get_cache_key(datasource3)


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_get_model(is_closed, close, connect, mocker, snowflake_datasource, snowflake_connector):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    mocked_common_get_databases = mocker.patch(
        "toucan_connectors.snowflake.snowflake_connector.SnowflakeCommon.get_databases",
        return_value=["booo"],
    )
    mocked_common_get_db_content = mocker.patch(
        "toucan_connectors.snowflake.snowflake_connector.SnowflakeCommon.get_db_content",
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
    res = snowflake_connector.get_model()
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


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_get_model_exception(
    is_closed, close, connect, mocker, snowflake_datasource, snowflake_connector
):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    mocked_common_get_databases = mocker.patch(
        "toucan_connectors.snowflake.snowflake_connector.SnowflakeCommon.get_databases",
        return_value=["booo"],
    )
    mocker.patch(
        "toucan_connectors.snowflake.snowflake_connector.SnowflakeCommon.get_db_content",
        side_effect=Exception,
    )

    with pytest.raises(Exception):
        snowflake_connector.get_model()
    mocked_common_get_databases.assert_called_once()
    cm.force_clean()


@patch("snowflake.connector.connect", return_value=SnowflakeConnection)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_close", return_value=True)
@patch("toucan_connectors.connection_manager.ConnectionBO.exec_alive", return_value=True)
def test_get_model_db_name_provided(
    is_closed, close, connect, mocker, snowflake_datasource, snowflake_connector
):
    cm = SnowflakeConnector.get_snowflake_connection_manager()
    mocked_common_get_databases = mocker.patch(
        "toucan_connectors.snowflake.snowflake_connector.SnowflakeCommon.get_databases",
        return_value=["booo"],
    )
    mocked_common_get_db_content = mocker.patch(
        "toucan_connectors.snowflake.snowflake_connector.SnowflakeCommon.get_db_content",
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
    res = snowflake_connector.get_model("SNOWFLAKE_SAMPLE_DATA")
    mocked_common_get_databases.assert_not_called()
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


@pytest.fixture
def keypair_authenticated_snowflake_connector() -> SnowflakeConnector:
    return SnowflakeConnector(
        name="sf-connector",
        user=environ["SNOWFLAKE_USER"],
        password=environ["SNOWFLAKE_PASSWORD"],
        account=environ["SNOWFLAKE_ACCOUNT"],
        private_key=environ["SNOWFLAKE_PRIVATE_KEY"],
        authentication_method=AuthenticationMethod.KEYPAIR,
    )


@pytest.fixture
def keypair_authenticated_snowflake_datasource() -> SnowflakeDataSource:
    return SnowflakeDataSource(
        domain="sf-domain",
        name="sf-datasource",
        database=environ["SNOWFLAKE_DATABASE"],
        warehouse=environ["SNOWFLAKE_WAREHOUSE"],
        query="SELECT 1;",
    )


@pytest.fixture
def beers_tiny_df() -> pd.DataFrame:
    path = Path(__file__).parent / "fixture" / "beers_tiny.csv"
    df = pd.read_csv(path)
    df["brewing_date"] = pd.to_datetime(df["brewing_date"], utc=True)
    return df


def test_retrieve_snowflake_data(
    keypair_authenticated_snowflake_connector: SnowflakeConnector,
    keypair_authenticated_snowflake_datasource: SnowflakeDataSource,
    beers_tiny_df: pd.DataFrame,
) -> None:
    conn = keypair_authenticated_snowflake_connector
    ds = keypair_authenticated_snowflake_datasource.copy(
        update={"query": 'SELECT * FROM TOUCAN_INTEGRATION_TESTS.INTEGRATION_TESTS."beers_tiny";'}
    )
    df = conn.get_df(ds)
    assert_frame_equal(df, beers_tiny_df)
