from typing import Any

import pandas as pd
import pymysql
import pytest
from pydantic.error_wrappers import ValidationError
from pytest_mock import MockerFixture

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.mysql.mysql_connector import (
    MySQLConnector,
    MySQLDataSource,
    NoQuerySpecified,
    handle_date_0,
)
from toucan_connectors.toucan_connector import MalformedVersion, UnavailableVersion


@pytest.fixture(scope="module")
def mysql_server(service_container):
    def check(host_port):
        conn = pymysql.connect(host="127.0.0.1", port=host_port, user="ubuntu", password="ilovetoucan")
        cur = conn.cursor()
        cur.execute("SELECT 1;")
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
    connector = MySQLConnector(name="my_mysql_con", host="myhost", user="myuser")
    params = connector.get_connection_params()
    params.pop("conv")
    assert params == {
        "host": "myhost",
        "user": "myuser",
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
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
        "cursorclass": pymysql.cursors.DictCursor,
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


def test_get_status_bad_authentication(mysql_connector):
    mysql_connector.user = "pika"
    assert mysql_connector.get_status() == ConnectorStatus(
        status=False,
        details=[
            ("Hostname resolved", True),
            ("Port opened", True),
            ("Host connection", True),
            ("Authenticated", False),
        ],
        error="Access denied for user 'pika'@'172.17.0.1' (using password: YES)",
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
        "AND test 'ok22%%' AND test LIKE '%(var)s' OR test LIKE '%%test' "
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
        }
    )
    mysql_connector.get_df(data_source)
    reasq.assert_called_once_with(
        expected_query_str,
        con=snock(),
        params={},
    )


def test_get_df_db(mysql_connector):
    """ " It should extract the table City without merges"""
    data_source_spec = {
        "domain": "MySQL test",
        "type": "external_database",
        "name": "Some MySQL provider",
        "database": "mysql_db",
        "query": "SELECT * FROM City WHERE Population > %(max_pop)s",
        "parameters": {"max_pop": 5000000},
    }

    expected_columns = {"ID", "Name", "CountryCode", "District", "Population"}
    data_source = MySQLDataSource(**data_source_spec)
    df = mysql_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_forbidden_table_interpolation(mysql_connector):
    data_source_spec = {
        "domain": "MySQL test",
        "type": "external_database",
        "name": "Some MySQL provider",
        "database": "mysql_db",
        "query": "SELECT * FROM %(tablename)s WHERE Population > 5000000",
        "parameters": {"tablename": "City"},
    }

    data_source = MySQLDataSource(**data_source_spec)
    with pytest.raises(pd.io.sql.DatabaseError) as e:
        mysql_connector.get_df(data_source)
    assert "interpolating table name is forbidden" in str(e.value)


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
