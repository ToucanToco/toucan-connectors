from os import environ

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from tenacity import retry, stop_after_delay, wait_fixed

from toucan_connectors.azure_mssql.azure_mssql_connector import (
    AzureMSSQLConnector,
    AzureMSSQLDataSource,
)


@pytest.fixture
def connector() -> AzureMSSQLConnector:
    user = environ["AZURE_MSSQL_USER"]
    password = environ["AZURE_MSSQL_PASSWORD"]
    host = environ["AZURE_MSSQL_HOST"]
    return AzureMSSQLConnector(name="azure-mssql-ci", user=user, host=host, password=password, connect_timeout=3)


@pytest.fixture
def datasource() -> AzureMSSQLDataSource:
    database = environ["AZURE_MSSQL_DATABASE"]
    return AzureMSSQLDataSource(domain="azure-mssql-ci", name="Azure MSSQL CI", database=database, query="SELECT 1;")


# Retrying every 5 seconds for 60 seconds
@retry(stop=stop_after_delay(60), wait=wait_fixed(5))
def _ready_connector(connector: AzureMSSQLConnector, datasource: AzureMSSQLDataSource) -> AzureMSSQLConnector:
    datasource = datasource.model_copy(update={"query": 'SELECT 1 "1";'})
    df = connector._retrieve_data(datasource)
    assert_frame_equal(df, pd.DataFrame({"1": [1]}))
    return connector


@pytest.fixture
def ready_connector(connector: AzureMSSQLConnector, datasource: AzureMSSQLDataSource) -> AzureMSSQLConnector:
    return _ready_connector(connector, datasource)


def test_azure_get_df_simple(ready_connector: AzureMSSQLConnector, datasource: AzureMSSQLDataSource) -> None:
    datasource.query = "SELECT name, population FROM City WHERE name = 'Maastricht';"
    df = ready_connector.get_df(datasource)
    expected = pd.DataFrame({"name": ["Maastricht"], "population": [122_087]})
    assert_frame_equal(df, expected)


def test_azure_get_df_with_parameters_and_modulo(
    ready_connector: AzureMSSQLConnector, datasource: AzureMSSQLDataSource
) -> None:
    datasource.query = "SELECT * FROM City WHERE CountryCode = {{ Code }} AND Population % 1000 >= 700"
    datasource.parameters = {"Code": "AFG"}
    df = ready_connector.get_df(datasource)
    assert_frame_equal(
        df,
        pd.DataFrame(
            {
                "ID": [3, 4],
                "Name": ["Herat", "Mazar-e-Sharif"],
                "CountryCode": ["AFG", "AFG"],
                "District": ["Herat", "Balkh"],
                "Population": [186_800, 127_800],
            }
        ),
    )
