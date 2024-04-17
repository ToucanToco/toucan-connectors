import clickhouse_driver
import pandas as pd
import pytest
from pydantic import ValidationError

from toucan_connectors.clickhouse.clickhouse_connector import (
    ClickhouseConnector,
    ClickhouseDataSource,
)


@pytest.fixture(scope="module")
def clickhouse_server(service_container):
    def insert(host_port):
        connection = clickhouse_driver.connect(
            host="127.0.0.1",
            port=host_port,
            database="clickhouse_db",
            user="ubuntu",
            password="ilovetoucan",
        )
        cur = connection.cursor()
        cur.execute(
            """INSERT into clickhouse_db.city values (3986,'Palmdale','USA','California',116670), (3999,
            'Simi Valley','USA','California',111351), (3958,'Orange','USA','California',128821) """
        )
        cur.close()
        connection.close()

    return service_container("clickhouse", insert)


@pytest.fixture
def clickhouse_connector(clickhouse_server):
    return ClickhouseConnector(
        name="test",
        host="127.0.0.1",
        user="ubuntu",
        password="ilovetoucan",
        port=clickhouse_server["port"],
    )


def test_no_user():
    """It should raise an error as no user is given"""
    with pytest.raises(ValidationError):
        ClickhouseConnector(host="some_host", name="test")


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        ClickhouseDataSource(domain="test", name="test", database="clickhouse_db", query="")


def test_datasource():
    with pytest.raises(ValidationError):
        ClickhouseDataSource(name="mycon", domain="mydomain", database="clickhouse_db", query="")

    with pytest.raises(ValueError) as exc_info:
        ClickhouseDataSource(name="mycon", domain="mydomain", database="clickhouse_db")
    assert "'query' or 'table' must be set" in str(exc_info.value)

    ds = ClickhouseDataSource(name="mycon", domain="mydomain", database="clickhouse_db", table="test")
    assert ds.query == "select * from test;"


def test_clickhouse_get_df(mocker):
    mockonnect = mocker.patch("clickhouse_driver.connect")
    mocksql = mocker.patch("pandas.read_sql")

    clickhouse_connector = ClickhouseConnector(
        name="test", host="localhost", user="ubuntu", password="ilovetoucan", port=22
    )

    ds = ClickhouseDataSource(
        domain="test",
        name="test",
        database="clickhouse_db",
        query="SELECT Name, CountryCode, Population FROM City LIMIT 2;",
    )
    clickhouse_connector.get_df(ds)

    mockonnect.assert_called_once_with("clickhouse://ubuntu:ilovetoucan@localhost:22/clickhouse_db")

    mocksql.assert_called_once_with(
        "SELECT Name, CountryCode, Population FROM City LIMIT 2;", con=mockonnect(), params={}
    )


def test_retrieve_response(clickhouse_connector):
    """It should connect to the database and retrieve the response to the query"""
    ds = ClickhouseDataSource(
        domain="test",
        name="test",
        database="clickhouse_db",
        query="SELECT name, countrycode, population FROM city LIMIT 2;",
    )
    res = clickhouse_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert res.shape == (2, 3)


def test_get_df_db(clickhouse_connector):
    """It should extract the table City and make some merge with some foreign key."""
    data_source_spec = {
        "domain": "Clickhouse test",
        "type": "external_database",
        "name": "Some clickhouse provider",
        "database": "clickhouse_db",
        "query": "SELECT * FROM city WHERE population > %(max_pop)s",
        "parameters": {"max_pop": 10000},
    }
    expected_columns = {"id", "name", "countrycode", "district", "population"}
    data_source = ClickhouseDataSource(**data_source_spec)
    df = clickhouse_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (3, 5)


def test_get_df_array_interpolation(clickhouse_connector):
    data_source_spec = {
        "domain": "Clickhouse test",
        "type": "external_database",
        "name": "Some clickhouse provider",
        "database": "clickhouse_db",
        "query": "SELECT * FROM city WHERE id in %(ids)s",
        "parameters": {"ids": [3986, 3958]},
    }
    data_source = ClickhouseDataSource(**data_source_spec)
    df = clickhouse_connector.get_df(data_source)
    assert not df.empty
    assert df.shape == (2, 5)


def test_get_form_empty_query(clickhouse_connector):
    """It should raise an error has query and table are empty"""
    data_source_spec = {
        "domain": "Clickhouse test",
        "name": "Some clickhouse provider",
    }
    current_config = {}
    with pytest.raises(ValueError):
        ClickhouseDataSource(**data_source_spec).get_form(clickhouse_connector, current_config)


def test_get_form_query_with_good_database(clickhouse_connector):
    """It should give suggestions of the collections"""
    current_config = {"database": "clickhouse_db"}
    form = ClickhouseDataSource.get_form(clickhouse_connector, current_config)
    assert form["properties"]["database"] == {"$ref": "#/$defs/database"}
    assert form["$defs"]["database"] == {
        "title": "database",
        "enum": ["INFORMATION_SCHEMA", "clickhouse_db", "default", "information_schema"],
        "type": "string",
    }
    assert form["properties"]["table"] == {"allOf": [{"$ref": "#/$defs/table"}], "default": None}
    assert form["$defs"]["table"] == {"const": "city", "enum": ["city"], "title": "table", "type": "string"}
    assert form["required"] == ["domain", "name", "database"]


def test_get_form_connection_fails(mocker, clickhouse_connector):
    """It should return a form even if connect fails"""
    mocker.patch.object(clickhouse_driver, "connect").side_effect = IOError
    form = ClickhouseDataSource.get_form(clickhouse_connector, current_config={})
    assert "table" in form["properties"]


def test_model_json_schema():
    data_source_spec = {
        "domain": "Clickhouse test",
        "type": "external_database",
        "name": "Some clickhouse provider",
        "database": "clickhouse_db",
        "query": "SELECT * FROM city WHERE id in %(ids)s",
        "parameters": {"ids": [3986, 3958]},
    }
    ds = ClickhouseDataSource(**data_source_spec)
    assert list(ds.model_json_schema()["properties"].keys())[:4] == [
        "database",
        "table",
        "query",
        "parameters",
    ]


def test_create_connections():
    """Check that create_connection returns correctly formed connections URL"""
    c = ClickhouseConnector(
        name="test",
        host="127.0.0.1",
        user="ubuntu",
        password="ilovetoucan",
        port=9999,
    )
    assert c.get_connection_url() == "clickhouse://ubuntu:ilovetoucan@127.0.0.1:9999/default"
    c = ClickhouseConnector(
        name="test",
        host="127.0.0.1",
        user="ubuntu",
        password="ilovetoucan",
        port=9999,
        ssl_connection=True,
    )
    assert c.get_connection_url() == "clickhouses://ubuntu:ilovetoucan@127.0.0.1:9999/default"
