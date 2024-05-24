# ruff: noqa: E501
from datetime import datetime
from typing import Any

import pandas as pd
import pymysql
import pytest
from pandas.testing import assert_frame_equal
from pydantic.error_wrappers import ValidationError
from pytest_mock import MockerFixture

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.mysql.mysql_connector import (
    MySQLConnector,
    MySQLDataSource,
    NoQuerySpecified,
    _pyformat_params_to_jinja,
    handle_date_0,
    prepare_query_and_params_for_pymysql,
)
from toucan_connectors.toucan_connector import MalformedVersion, UnavailableVersion


@pytest.fixture(scope="module")
def mysql_server(service_container):
    def check(host_port):
        conn = pymysql.connect(
            host="127.0.0.1",
            port=host_port,
            user="ubuntu",
            password="ilovetoucan",
            database="mysql_db",
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM City LIMIT 1;")
        cur.close()
        conn.close()

    return service_container("mysql", check, pymysql.Error)


@pytest.fixture
def mysql_connector(mysql_server):
    return MySQLConnector(
        name="mycon",
        host="localhost",
        port=mysql_server["port"],
        user="ubuntu",
        password="ilovetoucan",
    )


def test_datasource():
    MySQLDataSource(name="mycon", domain="mydomain", database="mysql_db")
    MySQLDataSource(name="mycon", domain="mydomain", database="mysql_db", query="myquery")


def test_get_connection_params():
    expected_cursor_class = pymysql.cursors.Cursor if pd.__version__.startswith("2") else pymysql.cursors.DictCursor
    connector = MySQLConnector(name="my_mysql_con", host="myhost", user="myuser")
    params = connector.get_connection_params()
    params.pop("conv")
    assert params == {
        "host": "myhost",
        "user": "myuser",
        "charset": "utf8mb4",
        "cursorclass": expected_cursor_class,
    }

    connector = MySQLConnector(
        name="my_mssql_con",
        host="myhost",
        user="myuser",
        password="mypass",
        port=123,
        charset="utf8",
        connect_timeout=50,
    )
    params = connector.get_connection_params()
    params.pop("conv")
    assert params == {
        "host": "myhost",
        "user": "myuser",
        "charset": "utf8",
        "cursorclass": expected_cursor_class,
        "password": "mypass",
        "port": 123,
        "connect_timeout": 50,
    }


def test_get_status_all_good(mysql_connector):
    assert mysql_connector.get_status() == ConnectorStatus(
        status=True,
        details=[
            ("Hostname resolved", True),
            ("Port opened", True),
            ("Host connection", True),
            ("Authenticated", True),
        ],
    )


def test_get_engine_version(mocker, mysql_connector):
    mocked_connect = mocker.MagicMock()
    mocked_cursor = mocker.MagicMock()

    # Should be a valide semver version converted to tuple
    mocked_cursor.__enter__().fetchone.return_value = {"VERSION()": "3.4.5"}
    mocked_connect.cursor.return_value = mocked_cursor
    mocker.patch("toucan_connectors.mysql.mysql_connector.pymysql.connect", return_value=mocked_connect)
    assert mysql_connector.get_engine_version() == (3, 4, 5)

    # Should raise a MalformedVersion error
    mocked_cursor.__enter__().fetchone.return_value = {"VERSION()": "--bad-version-format-"}
    mocked_connect.cursor.return_value = mocked_cursor
    mocker.patch("toucan_connectors.mysql.mysql_connector.pymysql.connect", return_value=mocked_connect)
    with pytest.raises(MalformedVersion):
        assert mysql_connector.get_engine_version()

    # Should raise an UnavailableVersion error
    mocked_cursor.__enter__().fetchone.return_value = None
    mocked_connect.cursor.return_value = mocked_cursor
    mocker.patch("toucan_connectors.mysql.mysql_connector.pymysql.connect", return_value=mocked_connect)
    with pytest.raises(UnavailableVersion):
        assert mysql_connector.get_engine_version()


def test_get_status_bad_host(mysql_connector):
    mysql_connector.host = "localhot"
    status = mysql_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Hostname resolved", False),
        ("Port opened", None),
        ("Host connection", None),
        ("Authenticated", None),
    ]
    assert status.error is not None


def test_get_status_bad_port(mysql_connector):
    mysql_connector.port = 123000
    status = mysql_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Hostname resolved", True),
        ("Port opened", False),
        ("Host connection", None),
        ("Authenticated", None),
    ]
    assert "port must be 0-65535." in status.error


def test_get_status_bad_connection(mysql_connector, unused_port, mocker):
    mysql_connector.port = unused_port()
    mocker.patch("toucan_connectors.mysql.mysql_connector.MySQLConnector.check_port", return_value=True)
    status = mysql_connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Hostname resolved", True),
        ("Port opened", True),
        ("Host connection", False),
        ("Authenticated", None),
    ]
    assert status.error.startswith("Can't connect to MySQL server on 'localhost'")


def test_get_status_bad_authentication(mysql_connector: MySQLConnector, mocker: MockerFixture):
    mocker.patch.object(
        mysql_connector,
        "_connect",
        side_effect=pymysql.OperationalError(pymysql.constants.ER.ACCESS_DENIED_ERROR, "this is the error message"),
    )
    status = mysql_connector.get_status()

    assert status == ConnectorStatus(
        status=False,
        details=[
            ("Hostname resolved", True),
            ("Port opened", True),
            ("Host connection", True),
            ("Authenticated", False),
        ],
        error="this is the error message",
    )


def test_get_status_unknown_error(mysql_connector: MySQLConnector, mocker: MockerFixture):
    mocker.patch.object(
        mysql_connector,
        "_connect",
        side_effect=pymysql.OperationalError(pymysql.constants.CR.CR_COMPRESSION_WRONGLY_CONFIGURED, "this is bad"),
    )
    status = mysql_connector.get_status()

    # The host connection should be considered invalid on an unknown error
    assert status == ConnectorStatus(
        status=False,
        details=[
            ("Hostname resolved", True),
            ("Port opened", True),
            ("Host connection", False),
            ("Authenticated", None),
        ],
        error="this is bad",
    )


def test_get_df(mocker: MockerFixture):
    """It should call the sql extractor"""
    snock = mocker.patch("pymysql.connect")
    reasq = mocker.patch("pandas.read_sql")
    mysql_connector = MySQLConnector(name="mycon", host="localhost", port=22, user="ubuntu", password="ilovetoucan")

    # With query
    reasq.reset_mock()
    data_source = MySQLDataSource(
        **{
            "domain": "MySQL test",
            "type": "external_database",
            "name": "Some MySQL provider",
            "database": "mysql_db",
            "query": "select * from Country",
        }
    )
    mysql_connector.get_df(data_source)
    reasq.assert_called_once_with("select * from Country", con=snock(), params={})

    # With query having % and variables %(var)s
    query_str = (
        "select * from Country where test LIKE '%test example%' "
        "AND test 'ok22%' AND test LIKE '%(var)s' OR test LIKE '%test' "
        "OR test LIKE '(this is % a test'"
    )
    expected_query_str = (
        "select * from Country where test LIKE '%%test example%%' "
        "AND test 'ok22%%' AND test LIKE '%(__QUERY_PARAM_0__)s' OR test LIKE '%%test' "
        "OR test LIKE '(this is %% a test'"
    )
    reasq.reset_mock()
    data_source = MySQLDataSource(
        **{
            "domain": "MySQL test",
            "type": "external_database",
            "name": "Some MySQL provider",
            "database": "mysql_db",
            "query": query_str,
            "parameters": {"var": 42},
        }
    )
    mysql_connector.get_df(data_source)
    reasq.assert_called_once_with(
        expected_query_str,
        con=snock(),
        params={"__QUERY_PARAM_0__": 42},
    )


@pytest.fixture
def mysql_datasource() -> MySQLDataSource:
    return MySQLDataSource(
        **{
            "domain": "MySQL test",
            "type": "external_database",
            "name": "Some MySQL provider",
            "database": "mysql_db",
            "query": "SELECT * FROM City WHERE Population > {{max_pop}}",
            "parameters": {"max_pop": 5_000_000},
        }
    )


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM City WHERE Population > %(max_pop)s",
        "SELECT * FROM City WHERE Population > {{ max_pop }}",
    ],
)
def test_get_df_db(mysql_connector: MySQLConnector, mysql_datasource: MySQLDataSource, query: str):
    """It should extract the table City without merges.

    It should work with both jinja and pyformat paramstyle
    """

    mysql_datasource.query = query
    expected_columns = {"ID", "Name", "CountryCode", "District", "Population"}
    df = mysql_connector.get_df(mysql_datasource)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_with_dict_parameter(mysql_connector: MySQLConnector, mysql_datasource: MySQLDataSource):
    """It should work with dict parameters"""
    # different kinds of param formatting on purpose
    mysql_datasource.query = 'SELECT {{user.username}} "username", {{ today}} "today" FROM City WHERE Population > {{ user.attributes.population }}'
    mysql_datasource.parameters = {
        "user": {"username": "john@doe.com", "groups": [], "attributes": {"population": 5_000_000}},
        "today": datetime(2024, 5, 22, 12, 3),
    }

    df = mysql_connector.get_df(mysql_datasource)
    assert df.columns.to_list() == ["username", "today"]
    assert df["username"].to_list() == ["john@doe.com"] * 24
    assert df["today"].to_list() == ["2024-05-22 12:03:00"] * 24


def test_decode_df():
    """It should decode the bytes columns"""
    df = pd.DataFrame(
        {
            "date": [b"2013-08-01", b"2013-08-02"],
            "country": ["France", "Germany"],
            "number": [1, 2],
            "other": [b"pikka", b"chuuu"],
            "random": [3, 4],
        }
    )
    res = MySQLConnector.decode_df(df)
    assert res["date"].tolist() == ["2013-08-01", "2013-08-02"]
    assert res["other"].tolist() == ["pikka", "chuuu"]
    assert res[["country", "number", "random"]].equals(df[["country", "number", "random"]])

    df2 = df[["number", "random"]]
    res = MySQLConnector.decode_df(df2)
    assert res.equals(df2)


def test_get_form_empty_query(mysql_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = MySQLDataSource.get_form(mysql_connector, current_config)
    assert form["properties"]["database"] == {"$ref": "#/$defs/database"}
    assert form["$defs"]["database"] == {
        "title": "database",
        "enum": ["mysql_db", "other_db"],
        "type": "string",
    }


def test_get_form_query_with_good_database(mysql_connector):
    """It should give suggestions of the collections"""
    current_config = {"database": "mysql_db"}
    form = MySQLDataSource.get_form(mysql_connector, current_config)
    assert form["properties"]["database"] == {"$ref": "#/$defs/database"}
    assert form["$defs"]["database"] == {
        "title": "database",
        "type": "string",
        "enum": ["mysql_db", "other_db"],
    }
    assert form["required"] == ["domain", "name", "database"]


def test_handle_date_0():
    date_mixed_series = [pd.Timestamp("2021-06-23 12:34:56"), "0000-00-00 00:00:00"]
    df = pd.DataFrame({"DATE": date_mixed_series})

    # Initially, we have mixed dtype:
    assert df.dtypes.astype(str)["DATE"] == "object"

    df = handle_date_0(df)
    assert df.dtypes.astype(str)["DATE"] == "datetime64[ns]"
    assert list(df["DATE"]) == [pd.Timestamp("2021-06-23 12:34:56"), pd.NaT]


@pytest.mark.parametrize("db_name", (None, "mysql_db"))
def test_get_model(mysql_connector: Any, db_name: str | None) -> None:
    """Check that it returns the db tree structure"""
    res = mysql_connector.get_model(db_name=db_name)
    for r in res:
        r["columns"] = sorted(r["columns"], key=lambda c: c["name"])
    assert res == [
        {
            "schema": "mysql_db",
            "database": "mysql_db",
            "type": "table",
            "name": "City",
            "columns": [
                {"name": "CountryCode", "type": "char"},
                {"name": "District", "type": "char"},
                {"name": "ID", "type": "int"},
                {"name": "Name", "type": "char"},
                {"name": "Population", "type": "int"},
            ],
        },
        {
            "schema": "mysql_db",
            "database": "mysql_db",
            "type": "table",
            "name": "Country",
            "columns": [
                {"name": "Capital", "type": "int"},
                {"name": "Code", "type": "char"},
                {"name": "Code2", "type": "char"},
                {"name": "Continent", "type": "enum"},
                {"name": "GNP", "type": "float"},
                {"name": "GNPOld", "type": "float"},
                {"name": "GovernmentForm", "type": "char"},
                {"name": "HeadOfState", "type": "char"},
                {"name": "IndepYear", "type": "smallint"},
                {"name": "LifeExpectancy", "type": "float"},
                {"name": "LocalName", "type": "char"},
                {"name": "Name", "type": "char"},
                {"name": "Population", "type": "int"},
                {"name": "Region", "type": "char"},
                {"name": "SurfaceArea", "type": "float"},
            ],
        },
        {
            "schema": "mysql_db",
            "database": "mysql_db",
            "type": "table",
            "name": "CountryLanguage",
            "columns": [
                {"name": "CountryCode", "type": "char"},
                {"name": "IsOfficial", "type": "enum"},
                {"name": "Language", "type": "char"},
                {"name": "Percentage", "type": "float"},
            ],
        },
    ]


def test_get_model_other_db(mysql_connector: Any) -> None:
    """Check that it returns the db tree structure"""
    assert mysql_connector.get_model(db_name="other_db") == []


def test_get_model_non_existing_db(mysql_connector: Any) -> None:
    with pytest.raises(pymysql.err.OperationalError):
        mysql_connector.get_model(db_name="nope")


@pytest.mark.parametrize("query", ("   ", None))
def test_get_df_no_query(query: str, mocker: MockerFixture):
    mocker.patch("pymysql.connect")
    mocker.patch("pandas.read_sql")
    mysql_connector = MySQLConnector(name="mycon", host="localhost", port=22, user="ubuntu", password="ilovetoucan")

    with pytest.raises(NoQuerySpecified):
        mysql_connector.get_df(
            MySQLDataSource(
                **{
                    "domain": "MySQL test",
                    "type": "external_database",
                    "name": "Some MySQL provider",
                    "database": "mysql_db",
                    "query": query,
                }
            )
        )


def test_list_db_names_ensure_no_db_specified(mysql_connector: MySQLConnector, mocker: MockerFixture):
    connect_mock = mocker.patch("pymysql.connect")
    mysql_connector._list_db_names()
    assert connect_mock.call_count == 1
    assert "database" not in connect_mock.call_args.kwargs


def test_get_project_structure_no_parameter_ensure_no_db_name_specified(
    mysql_connector: MySQLConnector, mocker: MockerFixture
):
    connect_mock = mocker.patch("pymysql.connect")
    # Calling list to actually execute the function body
    list(mysql_connector._get_project_structure())
    assert connect_mock.call_count == 1
    assert "database" not in connect_mock.call_args.kwargs


def test_get_project_structure_no_parameter_with_db_name(mysql_connector: MySQLConnector, mocker: MockerFixture):
    connect_mock = mocker.patch("pymysql.connect")
    # Calling list to actually execute the function body
    list(mysql_connector._get_project_structure(db_name="something"))
    assert connect_mock.call_count == 1
    assert connect_mock.call_args.kwargs["database"] == "something"


@pytest.fixture
def mysql_connector_with_ssl():
    return MySQLConnector(
        name="mycon",
        host="localhost",
        port=3306,
        user="ubuntu",
        password="ilovetoucan",
        ssl_ca="-----BEGIN CERTIFICATE-----\nsomething\n-----END CERTIFICATE-----",
        ssl_cert="-----BEGIN CERTIFICATE-----\nsomething else\n-----END CERTIFICATE-----",
        ssl_key="-----BEGIN PRIVATE KEY-----\nand something else-----END PRIVATE KEY-----",
        ssl_mode="VERIFY_CA",
    )


@pytest.fixture
def mysql_connector_with_ssl_bundle():
    return MySQLConnector(
        name="mycon",
        host="localhost",
        port=3306,
        user="ubuntu",
        password="ilovetoucan",
        ssl_ca="-----BEGIN CERTIFICATE----- something -----END CERTIFICATE----- -----BEGIN CERTIFICATE----- something -----END CERTIFICATE-----",  # noqa: E501
        ssl_mode="VERIFY_CA",
    )


def test_ssl_parameters_verify_identity_errors():
    with pytest.raises(ValidationError):
        MySQLConnector(
            name="mycon",
            host="localhost",
            port=3306,
            user="ubuntu",
            password="ilovetoucan",
            ssl_cert="-----BEGIN CERTIFICATE-----\nsomething else\n-----END CERTIFICATE-----",
            ssl_key=None,
            ssl_mode="VERIFY_IDENTITY",
        )

    with pytest.raises(ValidationError):
        MySQLConnector(
            name="mycon",
            host="localhost",
            port=3306,
            user="ubuntu",
            password="ilovetoucan",
            ssl_cert=None,
            ssl_key="-----BEGIN PRIVATE KEY-----\nand something else-----END PRIVATE KEY-----",
            ssl_mode="VERIFY_IDENTITY",
        )


def test_ssl_parameters_verify_ca(mysql_connector_with_ssl: MySQLConnector, mocker: MockerFixture):
    connect_mock = mocker.patch("pymysql.connect")
    mysql_connector_with_ssl._connect()
    assert connect_mock.call_count == 1
    kwargs = connect_mock.call_args.kwargs
    assert kwargs["ssl_disabled"] is False
    assert kwargs["ssl_ca"] is not None
    assert kwargs["ssl_cert"] is not None
    assert kwargs["ssl_key"] is not None
    assert kwargs["ssl_verify_cert"] is True
    assert kwargs["ssl_verify_identity"] is False


def test_ssl_parameters_verify_identity(mysql_connector_with_ssl: MySQLConnector, mocker: MockerFixture):
    connect_mock = mocker.patch("pymysql.connect")
    mysql_connector_with_ssl.ssl_mode = "VERIFY_IDENTITY"
    mysql_connector_with_ssl._connect()
    assert connect_mock.call_count == 1
    kwargs = connect_mock.call_args.kwargs
    assert kwargs["ssl_disabled"] is False
    assert kwargs["ssl_ca"] is not None
    assert kwargs["ssl_cert"] is not None
    assert kwargs["ssl_key"] is not None
    assert kwargs["ssl_verify_cert"] is True
    assert kwargs["ssl_verify_identity"] is True


def test_ssl_parameters_verify_identity_with_pem_bundle(
    mysql_connector_with_ssl_bundle: MySQLConnector, mocker: MockerFixture
):
    connect_mock = mocker.patch("pymysql.connect")
    mysql_connector_with_ssl_bundle.ssl_mode = "VERIFY_IDENTITY"
    mysql_connector_with_ssl_bundle._connect()
    assert connect_mock.call_count == 1
    kwargs = connect_mock.call_args.kwargs
    assert kwargs["ssl_disabled"] is False
    assert kwargs["ssl_ca"] is not None
    assert "ssl_cert" not in kwargs
    assert "ssl_key" not in kwargs
    assert kwargs["ssl_verify_cert"] is True
    assert kwargs["ssl_verify_identity"] is True


def test_ssl_parameters_required_mode(mocker: MockerFixture):
    connect_mock = mocker.patch("pymysql.connect")
    conn = MySQLConnector(
        name="mycon",
        host="localhost",
        port=3306,
        user="ubuntu",
        password="ilovetoucan",
        ssl_mode="REQUIRED",
    )
    conn._connect()
    assert connect_mock.call_count == 1
    kwargs = connect_mock.call_args.kwargs
    assert kwargs["ssl_disabled"] is False
    assert kwargs["ssl_verify_cert"] is True
    for arg in ("ssl_ca", "ssl_cert", "ssl_key"):
        assert arg not in kwargs


_COMMON_PARAMS = {
    "max_pop": 5_000_000,
    "user": {
        "email": "john@doe.com",
        "attributes": {"age_years": 26, "fib": [1, 1, 2, 3, 5, 8]},
        "created_at": datetime(1997, 1, 1, 7, 8, 9),
    },
    "manif": datetime(2025, 5, 1),
}


@pytest.mark.parametrize(
    "query,params,expected_query,expected_params",
    [
        # simple pyformat
        (
            "SELECT * FROM City WHERE Population > %(max_pop)s",
            {"max_pop": 5_000_000},
            "SELECT * FROM City WHERE Population > %(__QUERY_PARAM_0__)s",
            {"__QUERY_PARAM_0__": 5_000_000},
        ),
        # simple jinja
        (
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
            {"max_pop": 5_000_000},
            "SELECT * FROM City WHERE Population > %(__QUERY_PARAM_0__)s",
            {"__QUERY_PARAM_0__": 5_000_000},
        ),
        # simple pyformat with "real-life" params
        (
            "SELECT * FROM City WHERE Population > %(max_pop)s",
            _COMMON_PARAMS,
            "SELECT * FROM City WHERE Population > %(__QUERY_PARAM_0__)s",
            {"__QUERY_PARAM_0__": 5_000_000},
        ),
        # simple jinja with "real-life" params
        (
            "SELECT * FROM City WHERE Population > {{max_pop}}",
            _COMMON_PARAMS,
            "SELECT * FROM City WHERE Population > %(__QUERY_PARAM_0__)s",
            {"__QUERY_PARAM_0__": 5_000_000},
        ),
        # repeated param. A double occurence should result in two distinct parameters
        (
            "SELECT {{max_pop}}, City.* FROM City WHERE Population > {{max_pop}}",
            _COMMON_PARAMS,
            "SELECT %(__QUERY_PARAM_0__)s, City.* FROM City WHERE Population > %(__QUERY_PARAM_1__)s",
            {"__QUERY_PARAM_0__": 5_000_000, "__QUERY_PARAM_1__": 5_000_000},
        ),
        # nesting and mixed jinja/qmark
        (
            """SELECT %(manif)s, {{ user['email']   }}, City.* FROM City WHERE LifeExpectancy > {{user.attributes["age_years"]}}""",
            _COMMON_PARAMS,
            "SELECT %(__QUERY_PARAM_0__)s, %(__QUERY_PARAM_1__)s, City.* FROM City WHERE LifeExpectancy > %(__QUERY_PARAM_2__)s",
            {
                "__QUERY_PARAM_0__": datetime(2025, 5, 1),
                "__QUERY_PARAM_1__": "john@doe.com",
                "__QUERY_PARAM_2__": 26,
            },
        ),
        # deep nesting
        (
            """SELECT %(user.email)s, {{user.attributes["age_years"]}}, {{ user.attributes.fib[2]}} FROM City WHERE LifeExpectancy > {{user.attributes.fib[4] * 10}}""",
            _COMMON_PARAMS,
            "SELECT %(__QUERY_PARAM_0__)s, %(__QUERY_PARAM_1__)s, %(__QUERY_PARAM_2__)s FROM City WHERE LifeExpectancy > %(__QUERY_PARAM_3__)s",
            {
                "__QUERY_PARAM_0__": "john@doe.com",
                "__QUERY_PARAM_1__": 26,
                "__QUERY_PARAM_2__": 2,
                "__QUERY_PARAM_3__": 50,  # 5 * 10
            },
        ),
    ],
)
def test_prepare_query_and_params_for_pymysql(
    query: str, params: dict[str, Any], expected_query: str, expected_params: dict[str, Any]
) -> None:
    query, params = prepare_query_and_params_for_pymysql(query, params)
    assert query == expected_query
    assert params == expected_params


@pytest.mark.parametrize(
    "query,expected_query",
    [
        (
            "SELECT * FROM City WHERE Population > %(max_pop)s",
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
        ),
        (
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
        ),
        (
            "SELECT %(max_pop)s, City.* FROM City WHERE Population > %(max_pop)d",
            "SELECT {{ max_pop }}, City.* FROM City WHERE Population > {{ max_pop }}",
        ),
        (
            """SELECT %( manif   )s, {{ user['email']   }}, City.* FROM City WHERE LifeExpectancy > {{user.attributes["age_years"]}}""",
            """SELECT {{ manif }}, {{ user['email']   }}, City.* FROM City WHERE LifeExpectancy > {{user.attributes["age_years"]}}""",
        ),
        (
            """SELECT %(user.email)s, {{user.attributes["age_years"]}}, {{ user.attributes.fib[2]}} FROM City WHERE LifeExpectancy > {{user.attributes.fib[4] * 10}}""",
            """SELECT {{ user.email }}, {{user.attributes["age_years"]}}, {{ user.attributes.fib[2]}} FROM City WHERE LifeExpectancy > {{user.attributes.fib[4] * 10}}""",
        ),
    ],
)
def test__pyformat_params_to_jinja(query: str, expected_query: str) -> None:
    assert _pyformat_params_to_jinja(query) == expected_query


@pytest.mark.parametrize(
    "query,params,expected",
    [
        # simple pyformat
        (
            "SELECT * FROM City WHERE Population > %(max_pop)s",
            {"max_pop": 5_000_000},
            {
                "CountryCode": ["BRA"],
                "District": ["Rio de Janeiro"],
                "ID": [207],
                "Name": ["Rio de Janeiro"],
                "Population": [5598953],
            },
        ),
        # simple jinja
        (
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
            {"max_pop": 5_000_000},
            {
                "CountryCode": ["BRA"],
                "District": ["Rio de Janeiro"],
                "ID": [207],
                "Name": ["Rio de Janeiro"],
                "Population": [5598953],
            },
        ),
        # simple pyformat with "real-life" params
        (
            "SELECT * FROM City WHERE Population > %(max_pop)s",
            _COMMON_PARAMS,
            {
                "CountryCode": ["BRA"],
                "District": ["Rio de Janeiro"],
                "ID": [207],
                "Name": ["Rio de Janeiro"],
                "Population": [5598953],
            },
        ),
        # simple jinja with "real-life" params
        (
            "SELECT * FROM City WHERE Population > {{max_pop}}",
            _COMMON_PARAMS,
            {
                "CountryCode": ["BRA"],
                "District": ["Rio de Janeiro"],
                "ID": [207],
                "Name": ["Rio de Janeiro"],
                "Population": [5598953],
            },
        ),
        # repeated param. A double occurence should result in two distinct parameters
        (
            "SELECT {{max_pop}}, City.* FROM City WHERE Population > {{max_pop}}",
            _COMMON_PARAMS,
            {
                "5000000": [5_000_000],
                "CountryCode": ["BRA"],
                "District": ["Rio de Janeiro"],
                "ID": [207],
                "Name": ["Rio de Janeiro"],
                "Population": [5598953],
            },
        ),
        # nesting and mixed jinja/qmark
        (
            """SELECT %(manif)s, {{ user['email']   }}, Country.Name FROM Country WHERE LifeExpectancy > {{user.attributes["age_years"]}}""",
            _COMMON_PARAMS,
            {
                "2025-05-01 00:00:00": ["2025-05-01 00:00:00"],
                "Name": ["Afghanistan"],
                "john@doe.com": ["john@doe.com"],
            },
        ),
        # deep nesting
        (
            """SELECT %(user.email)s, {{user.attributes["age_years"]}}, {{ user.attributes.fib[2]}}, Name FROM Country WHERE LifeExpectancy > {{user.attributes.fib[4] * 10}}""",
            _COMMON_PARAMS,
            # first country with LifeExpectancy > 50
            {"2": [2], "26": [26], "Name": ["Anguilla"], "john@doe.com": ["john@doe.com"]},
        ),
    ],
)
def test_get_slice_with_variables(
    query: str,
    params: dict[str, Any],
    expected: dict[str, Any],
    mysql_connector: MySQLConnector,
    mysql_datasource: MySQLDataSource,
) -> None:
    mysql_datasource.query = query
    mysql_datasource.parameters = params

    data_slice = mysql_connector.get_slice(mysql_datasource, limit=1, offset=1)
    as_dict = data_slice.df.to_dict(orient="list")
    assert as_dict == expected


def test_charset_collation(mysql_connector: MySQLConnector, mysql_datasource: MySQLDataSource) -> None:
    mysql_datasource.query = "SELECT @@character_set_database, @@collation_database, @@collation_connection;"
    # The DB & connection collection should match the server defaults (for MySQL 8)
    expected = {
        "@@character_set_database": ["utf8mb4"],
        "@@collation_database": ["utf8mb4_0900_ai_ci"],
        "@@collation_connection": ["utf8mb4_0900_ai_ci"],
    }

    df = mysql_connector.get_df(mysql_datasource)
    assert_frame_equal(df, pd.DataFrame(expected))

    # Setting the connection collation to mysql 5.7's and MariaDB's default
    mysql_connector.charset_collation = "utf8mb4_general_ci"
    expected["@@collation_connection"] = ["utf8mb4_general_ci"]

    df = mysql_connector.get_df(mysql_datasource)
    # the charset shouldn't be modified, nor should the db's collation. However, it should have been
    # updated for the connection
    assert_frame_equal(df, pd.DataFrame(expected))
