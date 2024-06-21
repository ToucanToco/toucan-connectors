import cx_Oracle
import pytest
from pytest_mock import MockerFixture

from toucan_connectors.oracle_sql.oracle_sql_connector import (
    OracleSQLConnector,
    OracleSQLDataSource,
)

missing_oracle_lib = False
try:
    cx_Oracle.connect()
except cx_Oracle.DatabaseError as e:
    missing_oracle_lib = "DPI-1047" in str(e)


@pytest.fixture(scope="module")
def oracle_server(service_container):
    def check(host_port):
        conn = cx_Oracle.connect(user="system", password="oracle", dsn=f"localhost:{host_port}/xe")
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM City")
        cursor.close()
        conn.close()

    # timeout is set to 5 min as the container takes a very long time to start
    return service_container("oraclesql", check, cx_Oracle.Error, timeout=300)


@pytest.fixture
def oracle_connector(oracle_server):
    return OracleSQLConnector(
        name="my_oracle_sql_con",
        user="system",
        password="oracle",
        dsn=f'localhost:{oracle_server["port"]}/xe',
    )


def test_oracle_get_df(mocker: MockerFixture):
    snock = mocker.patch("cx_Oracle.connect")
    reasq = mocker.patch("pandas.read_sql")
    oracle_connector = OracleSQLConnector(
        name="my_oracle_sql_con", user="system", password="oracle", dsn="localhost:22/xe"
    )

    # With query
    datasource = OracleSQLDataSource(domain="Oracle test", name="my_oracle_sql_con", query="SELECT * FROM City;")
    oracle_connector.get_df(datasource)

    snock.assert_called_once_with(user="system", password="oracle", dsn="localhost:22/xe")

    reasq.assert_called_once_with("SELECT * FROM City", con=snock(), params=[])

    # With table
    reasq.reset_mock()
    oracle_connector.get_df(OracleSQLDataSource(domain="Oracle test", name="my_oracle_sql_con", table="Nation"))

    reasq.assert_called_once_with("SELECT * FROM Nation", con=snock(), params=[])

    # With both: query must prevail
    reasq.reset_mock()
    oracle_connector.get_df(
        OracleSQLDataSource(
            domain="Oracle test",
            name="my_oracle_sql_con",
            table="Drinks",
            query="SELECT * FROM Food",
        )
    )

    reasq.assert_called_once_with("SELECT * FROM Food", con=snock(), params=[])


def test_oracle_get_df_with_variables(mocker):
    """It should connect to the database and retrieve the response to the query"""
    snock = mocker.patch("cx_Oracle.connect")
    reasq = mocker.patch("pandas.read_sql")
    oracle_connector = OracleSQLConnector(
        name="my_oracle_sql_con", user="system", password="oracle", dsn="localhost:22/xe"
    )
    ds = OracleSQLDataSource(
        query="SELECT * FROM City WHERE id > %(id_nb)s AND population < %(population)s;",
        domain="Oracle test",
        name="my_oracle_sql_con",
        table="Cities",
        parameters={"population": 40_000, "id_nb": 1},
    )
    oracle_connector.get_df(ds)
    snock.assert_called_once_with(user="system", password="oracle", dsn="localhost:22/xe")
    reasq.assert_called_once_with(
        "SELECT * FROM City WHERE id > :1 AND population < :2", con=snock(), params=[1, 40_000]
    )
    reasq.reset_mock()
    snock.reset_mock()

    # test with array value
    ds = OracleSQLDataSource(
        query="SELECT * FROM City WHERE name in %(names)s AND population < %(population)s;",
        domain="Oracle test",
        name="my_oracle_sql_con",
        table="Cities",
        parameters={"population": 40_000, "names": ["Manhattan", "Kabul", "TaTaTin"]},
    )
    oracle_connector.get_df(ds)
    snock.assert_called_once_with(user="system", password="oracle", dsn="localhost:22/xe")
    reasq.assert_called_once_with(
        "SELECT * FROM City WHERE name in (:1,:2,:3) AND population < :4",
        con=snock(),
        params=["Manhattan", "Kabul", "TaTaTin", 40_000],
    )


def test_oracle_get_df_with_variables_jinja_syntax(mocker):
    """It should connect to the database and retrieve the response to the query"""
    snock = mocker.patch("cx_Oracle.connect")
    reasq = mocker.patch("pandas.read_sql")
    oracle_connector = OracleSQLConnector(
        name="my_oracle_sql_con", user="system", password="oracle", dsn="localhost:22/xe"
    )
    ds = OracleSQLDataSource(
        query="SELECT * FROM City WHERE Name = {{ __front_var_0__ }}",
        domain="Oracle test",
        name="my_oracle_sql_con",
        table="Cities",
        parameters={"__front_var_0__": "Kabul"},
    )
    oracle_connector.get_df(ds)
    snock.assert_called_once_with(user="system", password="oracle", dsn="localhost:22/xe")
    reasq.assert_called_once_with("SELECT * FROM City WHERE Name = :1", con=snock(), params=["Kabul"])


def test_get_df_db(oracle_connector):
    """It should extract the table City and make some merge with some foreign key"""
    data_sources_spec = [
        {
            "domain": "Oracle test",
            "type": "external_database",
            "name": "my_oracle_sql_con",
            "query": "SELECT * FROM City;",
        }
    ]

    data_source = OracleSQLDataSource(**data_sources_spec[0])
    df = oracle_connector.get_df(data_source)

    assert not df.empty
    assert df.shape == (50, 5)
    assert set(df.columns) == {"ID", "NAME", "COUNTRYCODE", "DISTRICT", "POPULATION"}

    assert len(df[df["POPULATION"] > 500000]) == 5


@pytest.mark.parametrize(
    "query,parameters",
    [
        ("SELECT * FROM City WHERE population < %(population)s;", {"population": 2346}),
        ("SELECT * FROM City WHERE population < {{ __front_var_0__ }}", {"__front_var_0__": 2346}),
        ("SELECT * FROM City WHERE Name = %(name)s;", {"name": "Willemstad"}),
        ("SELECT * FROM City WHERE Name = {{ __front_var_0__ }}", {"__front_var_0__": "Willemstad"}),
    ],
)
def test_get_df_db_with_variable(oracle_connector, query, parameters):
    """It should extract the table City and make some merge with some foreign key"""
    data_sources_spec = [
        {
            "domain": "Oracle test",
            "type": "external_database",
            "name": "my_oracle_sql_con",
            "query": query,
            "parameters": parameters,
        }
    ]

    data_source = OracleSQLDataSource(**data_sources_spec[0])
    df = oracle_connector.get_df(data_source)

    assert not df.empty
    assert df.shape == (1, 5)
    assert set(df.columns) == {"ID", "NAME", "COUNTRYCODE", "DISTRICT", "POPULATION"}
    assert len(df) == 1
    assert df["POPULATION"][0] == 2345


def test_get_form_empty_query(oracle_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = OracleSQLDataSource.get_form(oracle_connector, current_config)
    assert "CITY" in form["$defs"]["table"]["enum"]
    assert form["required"] == ["domain", "name"]


def test_datasource():
    with pytest.raises(ValueError) as exc_info:
        OracleSQLDataSource(name="mycon", domain="mydomain")
        assert "'query' or 'table' must be set" in str(exc_info.value)

    ds = OracleSQLDataSource(name="mycon", domain="mydomain", table="test")
    assert ds.query == "SELECT * FROM test"
