from unittest.mock import call

import pyodbc
import pytest
import responses
from pydantic import ValidationError
from pytest_mock import MockFixture

from toucan_connectors.common import ClusterStartException, ConnectorStatus
from toucan_connectors.databricks.databricks_connector import (
    DatabricksConnector,
    DatabricksDataSource,
)

CONNECTION_STRING = (
    "Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so;"
    "Host=127.0.0.1;"
    "Port=443;"
    "HTTPPath=foo/path;"
    "ThriftTransport=2;"
    "SSL=1;"
    "AuthMech=3;"
    "UID=token;"
    "PWD=12345"
)

CONNECTION_STATUS_OK = ConnectorStatus(
    status=True,
    error=None,
    details=[
        ("Host resolved", True),
        ("Port opened", True),
        ("Connected to Databricks", True),
        ("Authenticated", True),
    ],
)


@pytest.fixture
def databricks_connector() -> DatabricksConnector:
    return DatabricksConnector(name="test", host="127.0.0.1", port="443", http_path="foo/path", pwd="12345")


def test__build_connection_string(databricks_connector: DatabricksConnector):
    assert databricks_connector._build_connection_string() == CONNECTION_STRING


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        DatabricksDataSource(domain="test", name="test", query="")


def test_databricks_get_df(mocker: MockFixture, databricks_connector: DatabricksConnector):
    mock_pyodbc_connect = mocker.patch("pyodbc.connect")
    mock_pandas_read_sql = mocker.patch("pandas.read_sql")

    ds = DatabricksDataSource(
        domain="test",
        name="test",
        query="SELECT * FROM City WHERE Population > {{ max_pop }}",
        parameters={"max_pop": 5000000},
    )
    databricks_connector.get_df(ds)
    mock_pyodbc_connect.assert_called_once_with(CONNECTION_STRING, autocommit=True, ansi=False)
    mock_pandas_read_sql.assert_called_once_with(
        "SELECT * FROM City WHERE Population > ?",
        con=mock_pyodbc_connect(),
        params=[5000000],
    )


def test_query_variability(mocker: MockFixture, databricks_connector: DatabricksConnector):
    """It should connect to the database and retrieve the response to the query"""
    mock_pyodbc_connect = mocker.patch("pyodbc.connect")
    mock_pandas_read_sql = mocker.patch("pandas.read_sql")

    ds = DatabricksDataSource(
        query="select * from test where id_nb > %(id_nb)s and price > %(price)s;",
        domain="test",
        name="test",
        parameters={"price": 10, "id_nb": 1},
    )

    databricks_connector.get_df(ds)

    mock_pandas_read_sql.assert_called_once_with(
        "select * from test where id_nb > ? and price > ?;",
        con=mock_pyodbc_connect(),
        params=[1, 10],
    )


def test_get_status_all(databricks_connector: DatabricksConnector, mocker: MockFixture):
    mocker.patch(
        "toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname",
        side_effect=Exception("host unavailable"),
    )
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error="host unavailable",
        details=[
            ("Host resolved", False),
            ("Port opened", None),
            ("Connected to Databricks", None),
            ("Authenticated", None),
        ],
    )
    mocker.patch(
        "toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port",
        side_effect=Exception("port closed"),
    )
    mocker.patch("toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname")
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error="port closed",
        details=[
            ("Host resolved", True),
            ("Port opened", False),
            ("Connected to Databricks", None),
            ("Authenticated", None),
        ],
    )
    mocker.patch(
        "pyodbc.connect",
        side_effect=pyodbc.Error("I don't know mate"),
    )
    mocker.patch("toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port")
    mocker.patch("toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname")
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error="I don't know mate",
        details=[
            ("Host resolved", True),
            ("Port opened", True),
            ("Connected to Databricks", False),
            ("Authenticated", None),
        ],
    )
    mocker.patch(
        "pyodbc.connect",
        side_effect=pyodbc.InterfaceError("Invalid credentials", "Authentication/authorization error occured"),
    )
    mocker.patch("toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port")
    mocker.patch("toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname")
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error="Invalid credentials",
        details=[
            ("Host resolved", True),
            ("Port opened", True),
            ("Connected to Databricks", True),
            ("Authenticated", False),
        ],
    )
    mocked_connect = mocker.patch("pyodbc.connect")
    mocker.patch("toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port")
    mocker.patch("toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname")
    assert databricks_connector.get_status() == CONNECTION_STATUS_OK
    assert mocked_connect.call_args_list[0] == call(
        "Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so;"
        "Host=127.0.0.1;Port=443;HTTPPath=foo/path;ThriftTransport=2;SSL=1;AuthMech=3;UID=token;PWD=12345",
        autocommit=True,
        ansi=False,
    )


@responses.activate
def test_cluster_methods(databricks_connector: DatabricksConnector, mocker: MockFixture) -> None:
    responses.add(
        method="GET",
        url="https://127.0.0.1/api/2.0/clusters/get",
        headers={"login": "token", "password": "12345"},
        match=[responses.matchers.json_params_matcher({"cluster_id": "path"})],
        json={"state": "TERMINATED"},
    )
    assert databricks_connector.get_cluster_state() == "TERMINATED"
    responses.add(
        method="POST",
        url="https://127.0.0.1/api/2.0/clusters/start",
        headers={"login": "token", "password": "12345"},
        match=[responses.matchers.json_params_matcher({"cluster_id": "path"})],
        json={},
    )
    databricks_connector.start_cluster()


@responses.activate
def test_cluster_start_failed(databricks_connector: DatabricksConnector, mocker: MockFixture) -> None:
    responses.add(
        method="POST",
        url="https://127.0.0.1/api/2.0/clusters/start",
        headers={"login": "token", "password": "12345"},
        match=[responses.matchers.json_params_matcher({"cluster_id": "path"})],
        json={"message": "Failed to start Databricks cluster"},
        status=400,
    )
    mockedlog = mocker.patch("toucan_connectors.databricks.databricks_connector._LOGGER.error")
    with pytest.raises(ClusterStartException):
        databricks_connector.start_cluster()
    mockedlog.assert_called_once_with("Error while starting cluster: Failed to start Databricks cluster")
