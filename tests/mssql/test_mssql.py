import os
from unittest.mock import ANY

import pandas as pd
import pydantic
import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from tests.conftest import DockerContainer, ServiceContainerStarter
from toucan_connectors.mssql.mssql_connector import MSSQLConnector, MSSQLDataSource


@pytest.fixture(scope="module", params=["mssql2019", "mssql2022"])
def mssql_server(service_container: ServiceContainerStarter, request: pytest.FixtureRequest) -> DockerContainer:
    def check_and_feed(host_port: int):
        """
        This method does not only check that the server is on
        but also feeds the database once it's up !
        """
        sa_engine = MSSQLConnector(
            name="mycon",
            host="localhost",
            port=host_port,
            user="SA",
            password="Il0veT0uc@n!",
            # This is a local server so we allow self-signed certificates
            trust_server_certificate=True,
        )._create_engine("master")

        with Session(sa_engine) as session:
            with session.connection() as conn:
                cur = conn.connection.cursor()
                cur.execute("SELECT 1;")

                # Feed the database
                sql_query_path = f"{os.path.dirname(__file__)}/fixtures/world.sql"
                with open(sql_query_path) as f:
                    sql_query = f.read()
                cur.execute(sql_query)
                conn.commit()

                cur.close()

    return service_container(request.param, check_and_feed, OperationalError)


@pytest.fixture
def mssql_connector(mssql_server: DockerContainer) -> MSSQLConnector:
    return MSSQLConnector(
        name="mycon",
        host="localhost",
        user="SA",
        password="Il0veT0uc@n!",
        port=mssql_server["port"],
        # This is a local server so we allow self-signed certificates
        trust_server_certificate=True,
    )


def test_datasource():
    with pytest.raises(pydantic.ValidationError):
        MSSQLDataSource(name="mycon", domain="mydomain", database="ubuntu", query="")

    with pytest.raises(ValueError) as exc_info:
        MSSQLDataSource(name="mycon", domain="mydomain", database="ubuntu")
    assert "'query' or 'table' must be set" in str(exc_info.value)

    ds = MSSQLDataSource(name="mycon", domain="mydomain", database="ubuntu", table="test")
    assert ds.query == "select * from test;"


def assert_get_df(
    mocker: MockerFixture,
    mssql_connector: MSSQLConnector,
    datasource: MSSQLDataSource,
    expected_query: str,
    expected_params: tuple,
    expected_df: pd.DataFrame,
):
    import toucan_connectors.mssql.mssql_connector as mod

    mock_pandas_read_sqlalchemy_query = mocker.spy(mod, "pandas_read_sqlalchemy_query")

    assert_frame_equal(mssql_connector.get_df(datasource), expected_df)

    mock_pandas_read_sqlalchemy_query.assert_called_once_with(
        query=expected_query,
        engine=ANY,
        params=expected_params,
    )


def test_get_df_without_params(mssql_connector: MSSQLConnector):
    """It should connect to the default database and retrieve the response to the query"""
    datasource = MSSQLDataSource(
        name="mycon",
        domain="mydomain",
        database="master",
        query="SELECT TRIM(Name) AS Name, CountryCode, Population FROM City WHERE ID BETWEEN 1 AND 3",
    )

    # LIMIT 2 is not possible for MSSQL
    assert_frame_equal(
        mssql_connector.get_df(datasource),
        pd.DataFrame(
            {
                "Name": ["Kabul", "Qandahar", "Herat"],
                "CountryCode": ["AFG", "AFG", "AFG"],
                "Population": [1780000, 237500, 186800],
            }
        ),
    )


def test_get_df_with_scalar_params(mssql_connector: MSSQLConnector, mocker: MockerFixture):
    """It should connect to the database and retrieve the response to the query"""
    datasource = MSSQLDataSource(
        name="mycon",
        domain="mydomain",
        database="master",
        query="SELECT TRIM(Name) AS Name, CountryCode, Population FROM City "
        "WHERE CountryCode = %(code)s AND Population > %(population)s;",
        parameters={"code": "AFG", "population": 1000000},
    )

    assert_get_df(
        mocker=mocker,
        mssql_connector=mssql_connector,
        datasource=datasource,
        expected_query="SELECT TRIM(Name) AS Name, CountryCode, Population FROM City WHERE "
        "CountryCode = ? AND Population > ?;",
        expected_params=("AFG", 1000000),
        expected_df=pd.DataFrame(
            {
                "Name": ["Kabul"],
                "CountryCode": ["AFG"],
                "Population": [1780000],
            }
        ),
    )


def test_get_df_with_array_param(mssql_connector: MSSQLConnector, mocker: MockerFixture):
    """It should connect to the database and retrieve the response to the query"""
    datasource = MSSQLDataSource(
        name="mycon",
        domain="mydomain",
        database="master",
        query="SELECT TRIM(Name) AS Name, CountryCode, Population FROM City WHERE Id IN %(ids)s;",
        parameters={"ids": [1, 3]},
    )

    assert_get_df(
        mocker=mocker,
        mssql_connector=mssql_connector,
        datasource=datasource,
        expected_query="SELECT TRIM(Name) AS Name, CountryCode, Population FROM City WHERE Id IN (?,?);",
        expected_params=(1, 3),
        expected_df=pd.DataFrame(
            {
                "Name": ["Kabul", "Herat"],
                "CountryCode": ["AFG", "AFG"],
                "Population": [1780000, 186800],
            }
        ),
    )


def test_get_form_empty_query(mssql_connector: MSSQLConnector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = MSSQLDataSource.get_form(mssql_connector, current_config)

    assert form["properties"]["database"] == {"$ref": "#/$defs/database"}
    assert form["$defs"]["database"] == {
        "title": "database",
        "type": "string",
        "enum": ["master", "tempdb", "model", "msdb"],
    }


def test_get_form_query_with_good_database(mssql_connector: MSSQLConnector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {"database": "master"}
    form = MSSQLDataSource.get_form(mssql_connector, current_config)

    assert form["properties"]["database"] == {"$ref": "#/$defs/database"}
    assert form["$defs"]["database"] == {
        "title": "database",
        "type": "string",
        "enum": ["master", "tempdb", "model", "msdb"],
    }
    assert form["properties"]["table"] == {"$ref": "#/$defs/table", "default": None}
    assert "City" in form["$defs"]["table"]["enum"]
    assert form["required"] == ["domain", "name", "database"]
