from datetime import datetime, timedelta
from os import environ
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from urllib.error import HTTPError

import jwt
import pandas as pd
import pytest
import snowflake
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pydantic import SecretStr, ValidationError
from pytest_mock import MockerFixture

from toucan_connectors import DataSlice
from toucan_connectors.common import ConnectorStatus
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.snowflake import (
    AuthenticationMethod,
    SnowflakeConnector,
    SnowflakeDataSource,
)
from toucan_connectors.snowflake.snowflake_connector import SnowflakeConnection

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
    sso_credentials_keeper = mocker.Mock(client_id=OAUTH_CLIENT_ID, client_secret=SecretStr(OAUTH_CLIENT_SECRET))
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
        query_object={"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]},
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )


@pytest.fixture
def snowflake_connect(mocker: MockerFixture) -> MagicMock:
    return mocker.patch.object(SnowflakeConnection, "connect")


class _SFCursor(MagicMock):
    def set_return_value(self, v: Any):
        self.execute.return_value.fetchall.return_value = v

    def set_side_effect(self, v: Any):
        self.execute.return_value.fetchall.side_effect = v

    def set_describe_return_value(self, v: Any):
        self.describe.return_value = v


@pytest.fixture
def snowflake_cursor(mocker: MockerFixture, snowflake_connect: MagicMock) -> _SFCursor:
    cursor = _SFCursor()
    mocker.patch.object(SnowflakeConnection, "cursor", return_value=cursor)
    return cursor


@pytest.fixture
def snowflake_retrieve_data(snowflake_cursor: _SFCursor) -> _SFCursor:
    with open("tests/snowflake/fixture/data.json") as fd:
        data = JsonWrapper.load(fd)

    snowflake_cursor.set_return_value(data)
    return snowflake_cursor


def test_datasource_get_databases(
    snowflake_datasource: SnowflakeDataSource,
    snowflake_connector: SnowflakeConnector,
    snowflake_cursor: _SFCursor,
):
    snowflake_cursor.set_return_value({"name": ["database_1", "database_2"]})
    result = snowflake_datasource._get_databases(snowflake_connector)
    snowflake_cursor.execute.assert_called_once_with("SHOW DATABASES", None)
    assert len(result) == 2
    assert result[1] == "database_2"
    assert snowflake_datasource.language == "sql"
    assert snowflake_datasource.query_object == {
        "schema": "SHOW_SCHEMA",
        "table": "MY_TABLE",
        "columns": ["col1", "col2"],
    }


def test_datasource_get_form(
    snowflake_datasource: SnowflakeDataSource,
    snowflake_connector: SnowflakeConnector,
    snowflake_cursor: _SFCursor,
):
    snowflake_cursor.set_side_effect([{"name": ["warehouse_1", "warehouse_2"]}, {"name": ["database_1", "database_2"]}])
    result = snowflake_datasource.get_form(snowflake_connector, {})
    assert "warehouse_1" == result["properties"]["warehouse"]["default"]


def test_set_warehouse(snowflake_datasource: SnowflakeDataSource, snowflake_connector: SnowflakeConnector):
    snowflake_datasource.warehouse = None
    new_data_source = snowflake_connector._set_warehouse(snowflake_datasource)
    assert new_data_source.warehouse == "warehouse_1"


# TODO: What should we do when no default warehouse is specified ? Should requests
# still be executed ?
def test_set_warehouse_without_default_warehouse(snowflake_datasource: SnowflakeDataSource):
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


def test_get_database_without_filter(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_return_value([{"name": "database_1"}, {"name": "database_2"}])
    result = snowflake_connector._get_databases()
    assert result[0] == "database_1"
    assert result[1] == "database_2"
    assert len(result) == 2


def test_get_database_with_filter_found(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_return_value([{"name": "database_1"}])
    result = snowflake_connector._get_databases("database_1")
    assert result[0] == "database_1"
    assert len(result) == 1


@pytest.mark.usefixtures("snowflake_cursor")
def test_get_database_with_filter_not_found(snowflake_connector: SnowflakeConnector):
    result = snowflake_connector._get_databases("database_3")
    assert len(result) == 0


def test_get_warehouse_without_filter(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_return_value([{"name": "warehouse_1"}, {"name": "warehouse_2"}])
    result = snowflake_connector._get_warehouses()
    assert result[0] == "warehouse_1"
    assert result[1] == "warehouse_2"


def test_get_warehouse_with_filter_found(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_return_value([{"name": "warehouse_1"}])
    result = snowflake_connector._get_warehouses("warehouse_1")
    assert result[0] == "warehouse_1"
    assert len(result) == 1


@pytest.mark.usefixtures("snowflake_cursor")
def test_get_warehouse_with_filter_not_found(snowflake_connector: SnowflakeConnector):
    result = snowflake_connector._get_warehouses("warehouse_3")
    assert len(result) == 0


_DF = None


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_retrieve_data(snowflake_connector: SnowflakeConnector, snowflake_datasource: SnowflakeDataSource):
    df_result: DataFrame = snowflake_connector._retrieve_data(snowflake_datasource)
    assert 11 == len(df_result)


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_retrieve_data_slice(snowflake_connector: SnowflakeConnector, snowflake_datasource: SnowflakeDataSource):
    df_result: DataSlice = snowflake_connector.get_slice(snowflake_datasource)
    assert 11 == len(df_result.df)


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_retrieve_data_slice_offset_limit(
    snowflake_connector: SnowflakeConnector, snowflake_datasource: SnowflakeDataSource
):
    df_result: DataSlice = snowflake_connector.get_slice(snowflake_datasource, offset=5, limit=3)
    assert 11 == len(df_result.df)
    assert df_result.pagination_info.pagination_info.type == "unknown_size"


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_retrieve_data_slice_too_much(
    snowflake_connector: SnowflakeConnector, snowflake_datasource: SnowflakeDataSource
):
    df_result: DataSlice = snowflake_connector.get_slice(snowflake_datasource, offset=10, limit=20)
    assert 11 == len(df_result.df)


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_retrieve_data_fetch(snowflake_connector: SnowflakeConnector, snowflake_datasource: SnowflakeDataSource):
    df_result = snowflake_connector._fetch_data(snowflake_datasource)
    assert 11 == len(df_result)


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_retrieve_data_fetch_offset_limit(
    snowflake_connector: SnowflakeConnector, snowflake_datasource: SnowflakeDataSource
):
    df_result = snowflake_connector._fetch_data(snowflake_datasource, offset=5, limit=3)
    assert 11 == len(df_result)


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_retrieve_data_fetch_too_much(
    snowflake_connector: SnowflakeConnector, snowflake_datasource: SnowflakeDataSource
):
    df_result = snowflake_connector._fetch_data(snowflake_datasource, offset=10, limit=20)
    assert 11 == len(df_result)


def test_schema_fields_order():
    schema_props_keys = list(JsonWrapper.loads(SnowflakeConnector.schema_json())["properties"].keys())
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


def test_get_status_all_good(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_return_value([{"name": "warehouse_1"}])
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=True, details=[("Connection to Snowflake", True), ("Default warehouse exists", True)]
    )


def test_get_status_without_warehouses(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_return_value([])
    connector_status = snowflake_connector.get_status()
    assert not connector_status.status


def test_get_status_account_nok(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_side_effect(snowflake.connector.errors.ProgrammingError("Account nok"))
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error="Account nok",
        details=[("Connection to Snowflake", False), ("Default warehouse exists", None)],
    )


def test_account_does_not_exists(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_side_effect(snowflake.connector.errors.OperationalError())
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error=f"Connection failed for the account '{snowflake_connector.account}', please check the Account field",
        details=[("Connection to Snowflake", False), ("Default warehouse exists", None)],
    )


def test_account_forbidden(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_side_effect(snowflake.connector.errors.ForbiddenError())
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error=f"Access forbidden, please check that you have access to the '{snowflake_connector.account}' account or try again later.",  # noqa: E501
        details=[("Connection to Snowflake", False), ("Default warehouse exists", None)],
    )


def test_account_failed_for_user(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_side_effect(snowflake.connector.errors.DatabaseError())
    result = snowflake_connector.get_status()
    assert result == ConnectorStatus(
        status=False,
        error=f"Connection failed for the user '{snowflake_connector.user}', please check your credentials",
        details=[("Connection to Snowflake", False), ("Default warehouse exists", None)],
    )


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_oauth_args_wrong_type_of_auth(
    snowflake_connector_oauth: SnowflakeConnector,
    snowflake_datasource: SnowflakeDataSource,
    mocker: MockerFixture,
):
    spy = mocker.spy(SnowflakeConnector, "_refresh_oauth_token")

    snowflake_connector_oauth.authentication_method = AuthenticationMethod.PLAIN
    snowflake_connector_oauth._retrieve_data(snowflake_datasource)
    assert spy.call_count == 0


def test_oauth_args_endpoint_not_200(
    snowflake_connector_oauth: SnowflakeConnector,
    snowflake_datasource: SnowflakeDataSource,
    mocker: MockerFixture,
):
    snowflake_connector_oauth.user_tokens_keeper.access_token = SecretStr(
        jwt.encode({"exp": datetime.now() - timedelta(hours=24)}, key="supersecret")
    )

    def fake_raise_for_status():
        raise HTTPError("url", 401, "Unauthorized", {}, None)

    post_mock = mocker.patch("requests.post")
    post_mock.return_value.ok = False
    post_mock.return_value.raise_for_status.side_effect = HTTPError("url", 401, "Unauthorized", {}, None)
    post_mock.return_value.status_code = 401

    with pytest.raises(Exception, match="HTTP Error 401: Unauthorized"):
        snowflake_connector_oauth._retrieve_data(snowflake_datasource)
    assert post_mock.call_count == 1


@pytest.mark.usefixtures("snowflake_retrieve_data")
def test_refresh_oauth_token(
    snowflake_connector_oauth: SnowflakeConnector,
    snowflake_datasource: SnowflakeDataSource,
    mocker: MockerFixture,
):
    # Expired JWT
    snowflake_connector_oauth.user_tokens_keeper.access_token = SecretStr(
        jwt.encode({"exp": datetime.now() - timedelta(hours=24)}, key="supersecret")
    )

    post_mock = mocker.patch("requests.post")
    post_mock.return_value.status_code = 201
    post_mock.return_value.ok = True
    post_mock.return_value.return_value = {"access_token": "token", "refresh_token": "token"}

    snowflake_connector_oauth._retrieve_data(snowflake_datasource)
    assert post_mock.call_count == 1

    post_mock.reset_mock()
    post_mock.return_value.raise_for_status.side_effect = HTTPError("url", 401, "Unauthorized", {}, None)
    # Invalid JWT
    snowflake_connector_oauth.user_tokens_keeper.access_token = SecretStr("PLOP")
    with pytest.raises(Exception, match="HTTP Error 401: Unauthorized"):
        snowflake_connector_oauth._retrieve_data(snowflake_datasource)
    assert post_mock.call_count == 1


def test_get_connection_connect_oauth(
    snowflake_connector_oauth: SnowflakeConnector,
    mocker: MockerFixture,
    snowflake_connect: MagicMock,
):
    refresh_mock = mocker.patch.object(SnowflakeConnector, "_refresh_oauth_token")
    # Method returns a context manager, so we need to enter
    snowflake_connector_oauth._get_connection("test_database", "test_warehouse").__enter__()
    refresh_mock.assert_called_once()

    call_args = snowflake_connect.call_args_list[0][1]

    assert call_args["account"] == "test_account"
    assert (
        call_args["token"]
        == "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjQyLCJzdWIiOiJzbm93Zmxha2VfdXNlciJ9.WIi6tM3WAGh7gSyxcNHl8fFyDDXymyeBIVG55MFufvw"  # noqa: E501
    )
    assert call_args["database"] == "test_database"
    assert call_args["warehouse"] == "test_warehouse"


def test_describe(
    snowflake_datasource: SnowflakeDataSource,
    snowflake_connector: SnowflakeConnector,
    snowflake_cursor: _SFCursor,
):
    resp = MagicMock()
    resp.name = "ts"
    resp.type_code = 4
    snowflake_cursor.set_describe_return_value([resp])
    assert snowflake_connector.describe(snowflake_datasource) == {"ts": "timestamp"}


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
        query_object={"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]},
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )
    key = snowflake_connector.get_cache_key(datasource)

    datasource2 = SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_1",
        warehouse="warehouse_1",
        query="test_query with %(foo)s and %(pokemon)s",
        query_object={"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]},
        parameters={"foo": "bar", "pokemon": "pikachu"},
    )
    key2 = snowflake_connector.get_cache_key(datasource2)

    assert key == key2

    datasource3 = SnowflakeDataSource(
        name="test_name",
        domain="test_domain",
        database="database_2",
        warehouse="warehouse_1",
        query="test_query with %(foo)s and %(pokemon)s",
        query_object={"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]},
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

    assert snowflake_connector.get_cache_key(datasource) != another_snowflake_connector.get_cache_key(datasource)
    assert snowflake_connector.get_cache_key(datasource2) != another_snowflake_connector.get_cache_key(datasource2)
    assert snowflake_connector.get_cache_key(datasource3) != another_snowflake_connector.get_cache_key(datasource3)


_EXPECTED_MODEL = {
    "name": "REGION",
    "schema": "TPCH_SF1000",
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


@pytest.fixture
def dbs() -> list[str]:
    return ["OTHER_DB", "SNOWFLAKE_SAMPLE_DATA"]


@pytest.fixture
def mocked_get_model(snowflake_cursor: _SFCursor, mocker: MockerFixture, dbs: list[str]):
    snowflake_cursor.set_side_effect(
        [
            [
                {
                    "DATABASE": db,
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
            for db in dbs
        ]
    )
    mocker.patch.object(SnowflakeConnector, "_get_databases", return_value=dbs)


@pytest.mark.usefixtures("mocked_get_model")
def test_get_model_single_db(snowflake_connector: SnowflakeConnector):
    res = snowflake_connector.get_model("OTHER_DB")
    assert res == [{**_EXPECTED_MODEL, "database": "OTHER_DB"}]


@pytest.mark.usefixtures("mocked_get_model")
def test_get_model_all_dbs(snowflake_connector: SnowflakeConnector, dbs: list[str]):
    res = snowflake_connector.get_model()
    assert res == [{**_EXPECTED_MODEL, "database": db} for db in dbs]


def test_get_model_exception(snowflake_connector: SnowflakeConnector, snowflake_cursor: _SFCursor):
    snowflake_cursor.set_side_effect(Exception)

    with pytest.raises(Exception):
        snowflake_connector.get_model()

    snowflake_cursor.execute.assert_called_once()


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
    df["brewing_date"] = pd.to_datetime(df["brewing_date"])
    return df


def test_retrieve_snowflake_data(
    keypair_authenticated_snowflake_connector: SnowflakeConnector,
    keypair_authenticated_snowflake_datasource: SnowflakeDataSource,
    beers_tiny_df: pd.DataFrame,
) -> None:
    conn = keypair_authenticated_snowflake_connector
    ds = keypair_authenticated_snowflake_datasource.model_copy(
        update={"query": 'SELECT * FROM TOUCAN_INTEGRATION_TESTS.INTEGRATION_TESTS."beers_tiny";'}
    )

    df = conn.get_df(ds)
    assert_frame_equal(df, beers_tiny_df)


def test_instanciate_snowflake_connector_keypair_no_key() -> None:
    with pytest.raises(ValidationError, match="private_key must be specified"):
        SnowflakeConnector(
            name="sf-connector",
            user="user",
            password="password",
            account="account",
            authentication_method=AuthenticationMethod.KEYPAIR,
        )
