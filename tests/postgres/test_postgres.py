import pandas as pd
import psycopg
import pytest
from pydantic import ValidationError
from pytest_mock import MockFixture
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.postgres.postgresql_connector import (
    PostgresConnector,
    PostgresDataSource,
)
from toucan_connectors.toucan_connector import MalformedVersion


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
def postgres_connector(postgres_server) -> PostgresConnector:
    return PostgresConnector(
        name="test",
        host="localhost",
        user="ubuntu",
        password="ilovetoucan",
        default_database="postgres_db",
        port=postgres_server["port"],
    )


@pytest.fixture
def postgres_db_model() -> list[dict]:
    return [
        {
            "schema": "other_schema",
            "database": "postgres_db",
            "type": "table",
            "name": "city",
            "columns": [
                {"name": "code_pays", "type": "character"},
                {"name": "districteuh", "type": "text"},
                {"name": "id", "type": "integer"},
                {"name": "nom", "type": "text"},
                {"name": "populationg", "type": "integer"},
            ],
        },
        {
            "schema": "public",
            "database": "postgres_db",
            "type": "table",
            "name": "city",
            "columns": [
                {"name": "countrycode", "type": "character"},
                {"name": "district", "type": "text"},
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "text"},
                {"name": "population", "type": "integer"},
            ],
        },
        {
            "schema": "public",
            "database": "postgres_db",
            "type": "table",
            "name": "country",
            "columns": [
                {"name": "capital", "type": "integer"},
                {"name": "code", "type": "character"},
                {"name": "code2", "type": "character"},
                {"name": "continent", "type": "text"},
                {"name": "gnp", "type": "numeric"},
                {"name": "gnpold", "type": "numeric"},
                {"name": "governmentform", "type": "text"},
                {"name": "headofstate", "type": "text"},
                {"name": "indepyear", "type": "smallint"},
                {"name": "lifeexpectancy", "type": "real"},
                {"name": "localname", "type": "text"},
                {"name": "name", "type": "text"},
                {"name": "population", "type": "integer"},
                {"name": "region", "type": "text"},
                {"name": "surfacearea", "type": "real"},
            ],
        },
        {
            "schema": "public",
            "database": "postgres_db",
            "type": "table",
            "name": "countrylanguage",
            "columns": [
                {"name": "countrycode", "type": "character"},
                {"name": "isofficial", "type": "boolean"},
                {"name": "language", "type": "text"},
                {"name": "percentage", "type": "real"},
            ],
        },
    ]


@pytest.fixture
def postgres_db_model_with_materialized_views(postgres_db_model: list[dict]) -> list[dict]:
    return (
        [postgres_db_model[0]]
        + [
            {
                "schema": "other_schema",
                "database": "postgres_db",
                "type": "view",
                "name": "city_materialized_view",
                "columns": [
                    {"name": "code_pays", "type": "character"},
                    {"name": "districteuh", "type": "text"},
                    {"name": "id", "type": "integer"},
                    {"name": "nom", "type": "text"},
                    {"name": "populationg", "type": "integer"},
                ],
            },
        ]
        + postgres_db_model[1:]
        + [
            {
                "database": "postgres_db",
                "name": "country_materialized_view",
                "schema": "public",
                "type": "view",
                "columns": [
                    {"name": "capital", "type": "integer"},
                    {"name": "code", "type": "character"},
                    {"name": "code2", "type": "character"},
                    {"name": "continent", "type": "text"},
                    {"name": "gnp", "type": "numeric"},
                    {"name": "gnpold", "type": "numeric"},
                    {"name": "governmentform", "type": "text"},
                    {"name": "headofstate", "type": "text"},
                    {"name": "indepyear", "type": "smallint"},
                    {"name": "lifeexpectancy", "type": "real"},
                    {"name": "localname", "type": "text"},
                    {"name": "name", "type": "text"},
                    {"name": "population", "type": "integer"},
                    {"name": "region", "type": "text"},
                    {"name": "surfacearea", "type": "real"},
                ],
            }
        ]
    )


def test_get_status_all_good(postgres_connector):
    assert postgres_connector.get_status() == ConnectorStatus(
        status=True,
        details=[
            ("Host resolved", True),
            ("Port opened", True),
            ("Connected to PostgreSQL", True),
            ("Authenticated", True),
            ("Default Database connection", True),
        ],
    )


def test_get_engine_version(postgres_connector):
    # Should be a valide semver version converted to tuple
    version = postgres_connector.get_engine_version()
    assert isinstance(version, tuple)
    assert version[0] >= 16

    # Should raise a MalformedVersion error
    with pytest.raises(MalformedVersion):
        postgres_connector._format_version("--bad-version-format-")


def test_get_status_bad_host(postgres_connector):
    postgres_connector.host = "bad_host"
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Host resolved", False),
        ("Port opened", None),
        ("Connected to PostgreSQL", None),
        ("Authenticated", None),
        ("Default Database connection", None),
    ]
    assert status.error == "[Errno -3] Temporary failure in name resolution"


def test_get_status_bad_port(postgres_connector, unused_port):
    postgres_connector.port = unused_port()
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Host resolved", True),
        ("Port opened", False),
        ("Connected to PostgreSQL", None),
        ("Authenticated", None),
        ("Default Database connection", None),
    ]
    assert status.error == "[Errno 111] Connection refused"


def test_get_status_bad_connection(postgres_connector, unused_port, mocker):
    postgres_connector.port = unused_port()
    mocker.patch(
        "toucan_connectors.postgres.postgresql_connector.PostgresConnector.check_port",
        return_value=True,
    )
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Host resolved", True),
        ("Port opened", True),
        ("Connected to PostgreSQL", False),
        ("Authenticated", None),
        ("Default Database connection", None),
    ]
    assert "Connection refused" in status.error


def test_get_status_bad_authentication(postgres_connector):
    postgres_connector.user = "pika"
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Host resolved", True),
        ("Port opened", True),
        ("Connected to PostgreSQL", True),
        ("Authenticated", False),
        ("Default Database connection", None),
    ]
    assert 'password authentication failed for user "pika"' in status.error


def test_get_status_bad_default_database_connection(postgres_connector):
    postgres_connector.default_database = "zikzik"
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Host resolved", True),
        ("Port opened", True),
        ("Connected to PostgreSQL", True),
        # Auth should NOT work, as we should us the provided default database, not 'postgres'
        ("Authenticated", False),
        ("Default Database connection", None),
    ]
    assert 'database "zikzik" does not exist' in status.error


def test_no_user():
    """It should raise an error as no user is given"""
    with pytest.raises(ValidationError):
        PostgresConnector(host="some_host", name="test")


def test_open_connection():
    """It should not open a connection"""
    with pytest.raises(OperationalError):
        ds = PostgresDataSource(domain="pika", name="pika", database="circle_test", query="q")
        PostgresConnector(name="test", host="lolcathost", user="ubuntu", connect_timeout=1).get_df(ds)


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        PostgresDataSource(domaine="test", name="test", database="ubuntu", query="")


def test_datasource():
    with pytest.raises(ValidationError):
        PostgresDataSource(name="mycon", domain="mydomain", database="ubuntu", query="")

    with pytest.raises(ValueError) as exc_info:
        PostgresDataSource(name="mycon", domain="mydomain", database="ubuntu")
    assert "'query' or 'table' must be set" in str(exc_info.value)

    ds = PostgresDataSource(name="mycon", domain="mydomain", database="ubuntu", table="test")
    assert ds.query == "select * from test;"
    assert ds.language == "sql"
    assert hasattr(ds, "query_object")


def test_postgress_get_df(mocker, postgres_connector):
    reasq = mocker.patch("pandas.read_sql")
    ds = PostgresDataSource(
        domain="test",
        name="test",
        database="postgres_db",
        query="SELECT Name, CountryCode, Population FROM City Where Name Like '%test%' LIMIT 2;",
    )
    postgres_connector.get_df(ds)
    assert (
        str(reasq.call_args[0][0]) == "SELECT Name, CountryCode, Population FROM City Where Name Like '%test%' LIMIT 2;"
    )
    assert reasq.call_args[1] == {"params": {}}


def test_retrieve_response(postgres_connector):
    """It should connect to the database and retrieve the response to the query"""
    ds = PostgresDataSource(
        domain="test",
        name="test",
        database="postgres_db",
        query="SELECT Name, CountryCode, Population FROM City LIMIT 2;",
    )
    res = postgres_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert res.shape == (2, 3)


def test_get_df_db(postgres_connector):
    """It should extract the table City and make some merge with some foreign key."""
    data_source_spec = {
        "domain": "Postgres test",
        "type": "external_database",
        "name": "Some Postgres provider",
        "database": "postgres_db",
        "query": "SELECT * FROM City WHERE Population > %(max_pop)s",
        "parameters": {"max_pop": 5000000},
    }
    expected_columns = {"id", "name", "countrycode", "district", "population"}
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_db_jinja_syntax(postgres_connector):
    data_source_spec = {
        "domain": "Postgres test",
        "type": "external_database",
        "name": "Some Postgres provider",
        "database": "postgres_db",
        "query": "SELECT * FROM City WHERE Population > {{ max_pop }}",
        "parameters": {"max_pop": 5000000},
    }
    expected_columns = {"id", "name", "countrycode", "district", "population"}
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_forbidden_table_interpolation(postgres_connector):
    data_source_spec = {
        "domain": "Postgres test",
        "type": "external_database",
        "name": "Some Postgres provider",
        "database": "postgres_db",
        "query": "SELECT * FROM %(tablename)s WHERE Population > 5000000",
        "parameters": {"tablename": "City"},
    }
    data_source = PostgresDataSource(**data_source_spec)
    with pytest.raises(pd.errors.DatabaseError) as e:
        postgres_connector.get_df(data_source)
    assert "interpolating table name is forbidden" in str(e.value)


def test_get_df_array_interpolation(postgres_connector):
    data_source_spec = {
        "domain": "Postgres test",
        "type": "external_database",
        "name": "Some Postgres provider",
        "database": "postgres_db",
        "query": "SELECT * FROM City WHERE id = ANY(%(ids)s)",
        "parameters": {"ids": [1, 2]},
    }
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)
    assert not df.empty
    assert df.shape == (2, 5)


def test_get_form_empty_query(postgres_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = PostgresDataSource.get_form(postgres_connector, current_config)
    assert form["properties"]["database"] == {"$ref": "#/$defs/database"}
    assert form["$defs"]["database"] == {
        "title": "database",
        "type": "string",
        "enum": ["postgres", "postgres_db"],
    }


def test_get_form_query_with_good_database(postgres_connector, mocker):
    """It should give suggestions of the collections"""
    current_config = {"database": "postgres_db"}
    form = PostgresDataSource.get_form(postgres_connector, current_config)
    assert form["properties"]["database"] == {"$ref": "#/$defs/database"}
    assert form["$defs"]["database"] == {
        "title": "database",
        "type": "string",
        "enum": ["postgres", "postgres_db"],
    }
    assert form["properties"]["table"] == {"$ref": "#/$defs/table", "default": None}
    assert form["$defs"]["table"] == {
        "title": "table",
        "type": "string",
        "enum": ["city", "country", "countrylanguage"],
    }
    assert form["required"] == ["domain", "name", "database"]


def test_get_form_connection_fails(unused_port, postgres_connector):
    """It should return a form even if connect fails"""
    postgres_connector.port = unused_port()
    form = PostgresDataSource.get_form(postgres_connector, current_config={})
    assert "table" in form["properties"]


def test_describe(mocker, postgres_connector):
    """It should return a table description"""
    ds = PostgresDataSource(domain="test", name="test", database="postgres_db", query="SELECT * FROM city;")
    res = postgres_connector.describe(ds)
    assert res == {
        "id": "int4",
        "name": "text",
        "countrycode": "bpchar",
        "district": "text",
        "population": "int4",
    }


def test_describe_error(mocker, postgres_connector):
    """It should raise an error"""
    ds = PostgresDataSource(domain="test", name="test", database="postgres_db", query="SELECT * FROM invalid-table;")
    with pytest.raises(psycopg.ProgrammingError):
        postgres_connector.describe(ds)


def test_get_model(postgres_connector: PostgresConnector, postgres_db_model: list[dict]) -> None:
    """Check that it returns the db tree structure"""
    assert postgres_connector.get_model() == postgres_db_model
    assert postgres_connector.get_model(db_name="postgres_db") == postgres_db_model
    assert postgres_connector.get_model(db_name="another_db") == []


def test_get_model_exclude_columns(postgres_connector: PostgresConnector, postgres_db_model: list[dict]) -> None:
    """Check that it returns the db tree structure"""
    # We should not get any columns
    for elem in postgres_db_model:
        elem["columns"] = []
    assert postgres_connector.get_model(exclude_columns=True) == postgres_db_model
    assert postgres_connector.get_model(db_name="postgres_db", exclude_columns=True) == postgres_db_model
    assert postgres_connector.get_model(db_name="another_db", exclude_columns=True) == []


def test_get_model_with_table_and_schema(postgres_connector: PostgresConnector) -> None:
    city_tables = [
        {
            "schema": "other_schema",
            "database": "postgres_db",
            "type": "table",
            "name": "city",
            "columns": [
                {"name": "code_pays", "type": "character"},
                {"name": "districteuh", "type": "text"},
                {"name": "id", "type": "integer"},
                {"name": "nom", "type": "text"},
                {"name": "populationg", "type": "integer"},
            ],
        },
        {
            "schema": "public",
            "database": "postgres_db",
            "type": "table",
            "name": "city",
            "columns": [
                {"name": "countrycode", "type": "character"},
                {"name": "district", "type": "text"},
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "text"},
                {"name": "population", "type": "integer"},
            ],
        },
    ]

    other_schema_tables = [
        {
            "schema": "other_schema",
            "database": "postgres_db",
            "type": "table",
            "name": "city",
            "columns": [
                {"name": "code_pays", "type": "character"},
                {"name": "districteuh", "type": "text"},
                {"name": "id", "type": "integer"},
                {"name": "nom", "type": "text"},
                {"name": "populationg", "type": "integer"},
            ],
        },
        {
            "schema": "other_schema",
            "database": "postgres_db",
            "type": "view",
            "name": "city_materialized_view",
            "columns": [
                {"name": "code_pays", "type": "character"},
                {"name": "districteuh", "type": "text"},
                {"name": "id", "type": "integer"},
                {"name": "nom", "type": "text"},
                {"name": "populationg", "type": "integer"},
            ],
        },
    ]

    assert postgres_connector.get_model(table_name="city") == city_tables
    assert postgres_connector.get_model(table_name="city_materialized_view") == []
    assert postgres_connector.get_model(schema_name="other_schema") == [other_schema_tables[0]]
    assert postgres_connector.get_model(schema_name="other_schema", table_name="city") == [other_schema_tables[0]]
    assert postgres_connector.get_model(schema_name="other_schema", table_name="city_materialized_view") == []

    postgres_connector.include_materialized_views = True
    assert postgres_connector.get_model(table_name="city") == city_tables
    assert postgres_connector.get_model(table_name="city_materialized_view") == [other_schema_tables[1]]
    assert postgres_connector.get_model(schema_name="other_schema") == other_schema_tables
    assert postgres_connector.get_model(schema_name="other_schema", table_name="city") == [other_schema_tables[0]]
    assert postgres_connector.get_model(schema_name="other_schema", table_name="city_materialized_view") == [
        other_schema_tables[1]
    ]


def test_get_model_with_materialized_views(
    postgres_connector: PostgresConnector, postgres_db_model_with_materialized_views: list[dict]
) -> None:
    """Check that it returns the db tree structure"""
    postgres_connector.include_materialized_views = True
    assert postgres_connector.get_model() == postgres_db_model_with_materialized_views
    assert postgres_connector.get_model(db_name="postgres_db") == postgres_db_model_with_materialized_views
    assert postgres_connector.get_model(db_name="another_db") == []


def test_get_model_with_materialized_views_exclude_columns(
    postgres_connector: PostgresConnector, postgres_db_model_with_materialized_views: list[dict]
) -> None:
    # We should not get any columns
    for elem in postgres_db_model_with_materialized_views:
        elem["columns"] = []
    postgres_connector.include_materialized_views = True
    assert postgres_connector.get_model(exclude_columns=True) == postgres_db_model_with_materialized_views
    assert (
        postgres_connector.get_model(db_name="postgres_db", exclude_columns=True)
        == postgres_db_model_with_materialized_views
    )
    assert postgres_connector.get_model(db_name="another_db", exclude_columns=True) == []


def test_raised_error_for_get_model(mocker, postgres_connector):
    """Check that it returns the db tree structure"""
    with mocker.patch.object(
        PostgresConnector, "_list_tables_info", side_effect=OperationalError(statement=None, params=None, orig=None)
    ):
        assert postgres_connector.get_model() == []


def test_get_model_with_info(postgres_connector: PostgresConnector, postgres_db_model: list[dict]) -> None:
    """Check that it returns the db tree structure"""
    assert postgres_connector.get_model_with_info() == (postgres_db_model, {})
    assert postgres_connector.get_model_with_info(db_name="postgres_db") == (postgres_db_model, {})
    assert postgres_connector.get_model_with_info(db_name="another_db") == (
        [],
        {"info": {"Could not reach databases": ["another_db"]}},
    )


def test_raised_error_for_get_model_with_info(mocker, postgres_connector):
    """Check that it returns the db tree structure"""
    with mocker.patch.object(
        PostgresConnector, "_list_tables_info", side_effect=OperationalError(statement=None, params=None, orig=None)
    ):
        assert postgres_connector.get_model_with_info() == (
            [],
            {"info": {"Could not reach databases": ["postgres", "postgres_db"]}},
        )


def test_connection_is_established_with_right_default_database(
    mocker: MockFixture, postgres_connector: PostgresConnector
):
    def create_engine(self, database: str, connect_timeout: int):
        assert database == "d3f4ult"
        assert connect_timeout == 1
        raise SQLAlchemyError("cannot connect to d3f4ult database")

    mocker.patch.object(PostgresConnector, "create_engine", new=create_engine)
    postgres_connector.default_database = "d3f4ult"
    postgres_connector.get_status()
