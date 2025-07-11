import pandas as pd
import psycopg
import pyodbc
import pytest
from pydantic import ValidationError

from toucan_connectors.odbc.odbc_connector import OdbcConnector, OdbcDataSource


def test_postgres_driver_installed():
    """
    Check that pgodbc is installed
    """
    assert "PostgreSQL Unicode" in pyodbc.drivers()


@pytest.fixture(scope="module")
def postgres_server(service_container):
    def check(host_port):
        conn = psycopg.connect(f"postgres://ubuntu:ilovetoucan@127.0.0.1:{host_port}/postgres_db")
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()

    return service_container("postgres", check, psycopg.Error)


@pytest.fixture
def odbc_connector(postgres_server):
    return OdbcConnector(
        name="test",
        connection_string=(
            "DRIVER={PostgreSQL Unicode};"
            "DATABASE=postgres_db;"
            "UID=ubuntu;"
            "PWD=ilovetoucan;"
            "SERVER=127.0.0.1;"
            "PORT=" + str(postgres_server["port"]) + ";"
        ),
    )


def test_invalid_connection_string():
    """It should raise an error as the connection string is invalid"""
    with pytest.raises(ValidationError):
        OdbcConnector(name="test")


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        OdbcDataSource(domain="test", name="test", database="postgres_db", query="")


def test_odbc_connector_autocommit(mocker):
    """Test that we are passing the autocommit param properly"""
    mock_pyodbc_connect = mocker.patch("pyodbc.connect")
    mocker.patch("pandas.read_sql")

    odbc_connector = OdbcConnector(name="test", connection_string="blah", autocommit=True)
    ds = OdbcDataSource(
        domain="test",
        name="test",
        query="SELECT 1;",
    )
    odbc_connector.get_df(ds)

    mock_pyodbc_connect.assert_called_once_with("blah", autocommit=True, ansi=False)


def test_odbc_get_df(mocker):
    mock_pyodbc_connect = mocker.patch("pyodbc.connect")
    mock_pandas_read_sql = mocker.patch("pandas.read_sql")

    odbc_connector = OdbcConnector(name="test", connection_string="blah")

    ds = OdbcDataSource(
        domain="test",
        name="test",
        query="SELECT Name, CountryCode, Population from city LIMIT 2;",
    )
    odbc_connector.get_df(ds)
    mock_pyodbc_connect.assert_called_once_with("blah", autocommit=False, ansi=False)
    mock_pandas_read_sql.assert_called_once_with(
        "SELECT Name, CountryCode, Population from city LIMIT 2;",
        con=mock_pyodbc_connect(),
        params=[],
    )


@pytest.mark.skip()
def test_retrieve_response(odbc_connector):
    """It should connect to the database and retrieve the response to the query"""
    ds = OdbcDataSource(query="select * from City;", domain="test", name="test")
    res = odbc_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert res.shape[0] > 1


def test_query_variability(mocker):
    """It should connect to the database and retrieve the response to the query"""
    mock_pyodbc_connect = mocker.patch("pyodbc.connect")
    mock_pandas_read_sql = mocker.patch("pandas.read_sql")
    odbc_connector = OdbcConnector(name="test", connection_string="blah")

    ds = OdbcDataSource(
        query="select * from test where id_nb > %(id_nb)s and price > %(price)s;",
        domain="test",
        name="test",
        parameters={"price": 10, "id_nb": 1},
    )

    odbc_connector.get_df(ds)

    mock_pandas_read_sql.assert_called_once_with(
        "select * from test where id_nb > ? and price > ?;",
        con=mock_pyodbc_connect(),
        params=[1, 10],
    )
