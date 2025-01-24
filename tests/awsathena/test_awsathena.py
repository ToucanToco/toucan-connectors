import os
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pydantic import SecretStr
from pytest_mock import MockFixture

from toucan_connectors.awsathena.awsathena_connector import AwsathenaConnector, AwsathenaDataSource
from toucan_connectors.common import ConnectorStatus
from toucan_connectors.pagination import OffsetLimitInfo


@pytest.fixture
def athena_connector():
    return AwsathenaConnector(
        name="test",
        s3_output_bucket="s3://test/results/",
        aws_access_key_id="test_access_key_id",
        aws_secret_access_key=SecretStr("test_secret_access_key"),
        region_name="test-region",
    )


@pytest.fixture
def data_source():
    return AwsathenaDataSource(name="test", domain="toto", database="mydatabase", query="SELECT * FROM beers;")


@pytest.fixture
def sample_data() -> pd.DataFrame:
    fixture_path = f"{os.path.dirname(__file__)}/fixtures/beers.csv"
    return pd.read_csv(fixture_path)


@pytest.fixture
def mocked_read_sql_query(mocker, sample_data):
    return mocker.patch("awswrangler.athena.read_sql_query", return_value=sample_data)


@pytest.fixture
def mocked_boto_session(mocker):
    # Mocking because the comparison of two boto3.Session objects is always false
    # We cannot mock get_session on the athena_connector instance directly, because
    # pydantic models alter getattr behaviour
    return mocker.patch.object(AwsathenaConnector, "get_session", return_value={"a": "b"})


@pytest.fixture
def status_checks() -> list[str]:
    return [
        "Host resolved",
        "Port opened",
        "Connected",
        "Authenticated",
        "Can list databases",
    ]


@pytest.mark.parametrize("use_ctas", (True, False))
def test_get_df(
    athena_connector,
    data_source,
    sample_data,
    mocked_read_sql_query,
    mocked_boto_session,
    use_ctas: bool,
):
    data_source.use_ctas = use_ctas
    # The actual data request
    df = athena_connector.get_df(data_source=data_source)

    assert df.equals(sample_data)

    mocked_read_sql_query.assert_called_once_with(
        "SELECT * FROM beers;",
        params=None,
        database="mydatabase",
        boto3_session={"a": "b"},
        s3_output="s3://test/results/",
        ctas_approach=use_ctas,
        paramstyle="named",
    )

    mocked_boto_session.assert_called_once()


def test_get_session(athena_connector):
    sess = athena_connector.get_session()
    assert sess.region_name == "test-region"
    creds = sess.get_credentials()
    assert creds.access_key == "test_access_key_id"
    assert creds.secret_key == "test_secret_access_key"


def test_get_slice(mocker, athena_connector, data_source, mocked_read_sql_query, mocked_boto_session):
    permissions = {"column": "style", "operator": "in", "value": ["Blonde", "Triple"]}
    result = athena_connector.get_slice(data_source, permissions=permissions, offset=10, limit=110)
    assert len(result.df) == 4
    assert result.pagination_info.pagination_info.total_rows == 14
    assert result.pagination_info.parameters == OffsetLimitInfo(offset=10, limit=110)
    assert sorted(result.df["style"].unique().tolist()) == ["Blonde", "Triple"]
    mocked_read_sql_query.assert_called_once_with(
        "SELECT * FROM (SELECT * FROM beers) OFFSET 10 LIMIT 110;",
        params=None,
        database="mydatabase",
        boto3_session={"a": "b"},
        s3_output="s3://test/results/",
        ctas_approach=False,
        paramstyle="named",
    )


def test_get_status_list_dbs_ok(mocker, athena_connector, status_checks):
    mocker.patch("awswrangler.catalog.databases", return_value=pd.DataFrame())
    assert athena_connector.get_status() == ConnectorStatus(
        status=True, message=None, details=[(c, True) for c in status_checks], error=None
    )


def test_get_status_list_dbs_ko_sts_ok(mocker, athena_connector, status_checks):
    mocker.patch("awswrangler.catalog.databases", side_effect=Exception("coucou"))

    mocked_session = mocker.MagicMock()
    mocked_session.return_value.client.return_value.get_caller_identity.return_value = True
    mocker.patch.object(AwsathenaConnector, "get_session", new=mocked_session)

    expected_details = [(c, True) for c in status_checks]
    expected_details[4] = ("Can list databases", False)

    assert athena_connector.get_status() == ConnectorStatus(
        status=True, message=None, details=expected_details, error="Cannot list databases: coucou"
    )


def test_get_status_ko(mocker, athena_connector, status_checks):
    mocker.patch("awswrangler.catalog.databases", side_effect=Exception("Insufficient permissions"))

    mocked_session = mocker.MagicMock()
    mocked_session.return_value.client.return_value.get_caller_identity.side_effect = Exception("Authentication failed")
    mocker.patch.object(AwsathenaConnector, "get_session", new=mocked_session)

    # should failed
    assert athena_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error="Cannot verify connection to Athena: Insufficient permissions, Authentication failed",
        details=[(c, False) for c in status_checks],
    )


@pytest.mark.parametrize(
    "query,offset,limit,expected",
    [
        # no pagination
        ("SELECT * FROM toto;", 0, None, "SELECT * FROM toto;"),
        # limit only, whitespace, no trailing ;
        ("   SELECT * FROM toto ", 0, 100, "SELECT * FROM (SELECT * FROM toto) LIMIT 100;"),
        # limit only, whitespace, with trailing ;
        ("  SELECT * FROM toto; ", 0, 100, "SELECT * FROM (SELECT * FROM toto) LIMIT 100;"),
        # offset + limit, whitespace, no trailing ;
        ("  SELECT * FROM toto ", 5, 100, "SELECT * FROM (SELECT * FROM toto) OFFSET 5 LIMIT 100;"),
        # offset + limit, whitespace, trailing ;
        (" SELECT * FROM toto; ", 5, 100, "SELECT * FROM (SELECT * FROM toto) OFFSET 5 LIMIT 100;"),
    ],
)
def test_add_pagination_to_query(athena_connector, query: str, offset: int, limit: int | None, expected: str):
    assert athena_connector._add_pagination_to_query(query, offset=offset, limit=limit) == expected


def test_athenadatasource_get_form(
    mocker: MockFixture,
    athena_connector: AwsathenaConnector,
    data_source: AwsathenaDataSource,
    mocked_boto_session: MagicMock,
):
    current_config = {"database": "dev"}
    mocker.patch.object(AwsathenaConnector, "get_session")
    mocker.patch(
        "toucan_connectors.awsathena.awsathena_connector.wr.catalog.databases",
        return_value=pd.DataFrame({"Database": ["db1", "db2"]}),
    )

    result = data_source.get_form(athena_connector, current_config)
    assert result["properties"]["parameters"]["title"] == "Parameters"
    assert result["properties"]["domain"]["title"] == "Domain"
    assert result["properties"]["validation"]["title"] == "Validation"
    assert result["required"] == ["domain", "name", "database"]
    assert result["$defs"]["database"]["enum"] == ["db1", "db2"]


@pytest.mark.usefixtures("data_source", "mocked_boto_session")
def test_athenaconnector_get_model(mocker: MockFixture, athena_connector: AwsathenaConnector):
    mocker.patch.object(AwsathenaConnector, "get_session")
    dbs_mock = mocker.patch(
        "toucan_connectors.awsathena.awsathena_connector.wr.catalog.databases",
        return_value=pd.DataFrame({"Database": ["db1", "db2"]}),
    )
    tables_mock = mocker.patch(
        "toucan_connectors.awsathena.awsathena_connector.wr.catalog.tables",
        return_value=pd.DataFrame({"Table": ["table1", "table2"], "TableType": ["EXTERNAL_TABLE", "EXTERNAL_TABLE"]}),
    )
    table_types = [
        {"foo": "string", "bar": "string"},
        {"roo": "integer", "far": "datetime"},
        {"loo": "string", "rab": "string"},
        {"broo": "integer", "farf": "datetime"},
    ]
    table_types_mock = mocker.patch(
        "toucan_connectors.awsathena.awsathena_connector.wr.catalog.get_table_types",
        side_effect=table_types,
    )

    expected_model = [
        {
            "name": "table1",
            "database": "db1",
            "type": "table",
            "columns": [{"name": "foo", "type": "string"}, {"name": "bar", "type": "string"}],
        },
        {
            "name": "table2",
            "database": "db1",
            "type": "table",
            "columns": [{"name": "roo", "type": "integer"}, {"name": "far", "type": "datetime"}],
        },
        {
            "name": "table1",
            "database": "db2",
            "type": "table",
            "columns": [{"name": "loo", "type": "string"}, {"name": "rab", "type": "string"}],
        },
        {
            "name": "table2",
            "database": "db2",
            "type": "table",
            "columns": [{"name": "broo", "type": "integer"}, {"name": "farf", "type": "datetime"}],
        },
    ]
    assert athena_connector.get_model() == expected_model
    assert dbs_mock.call_count == 1
    assert tables_mock.call_count == 2
    assert table_types_mock.call_count == 4
    dbs_mock.reset_mock()
    tables_mock.reset_mock()
    table_types_mock.reset_mock()
    table_types_mock.side_effect = table_types

    assert athena_connector.get_model("db1") == expected_model[:2]
    dbs_mock.assert_not_called()
    assert tables_mock.call_count == 1
    assert table_types_mock.call_count == 2
    dbs_mock.reset_mock()
    tables_mock.reset_mock()
    table_types_mock.reset_mock()
    table_types_mock.side_effect = table_types[2:]

    assert athena_connector.get_model("db2") == expected_model[2:]
    dbs_mock.assert_not_called()
    assert tables_mock.call_count == 1
    assert table_types_mock.call_count == 2


def test_no_sql_injection(athena_connector, mocker):
    read_sql_mock: MagicMock = mocker.patch("awswrangler.athena.read_sql_query")

    ds = AwsathenaDataSource(
        name="test",
        domain="users",
        database="db",
        query="SELECT * FROM Users WHERE UserId = {{ user_id }}",
        parameters={"user_id": "105 OR 1=1"},
    )
    assert ds.query == "SELECT * FROM Users WHERE UserId = :__QUERY_PARAM_0__"
    assert ds.parameters == {"user_id": "105 OR 1=1", "__QUERY_PARAM_0__": "105 OR 1=1"}

    athena_connector.get_df(ds)
    read_sql_mock.assert_called_once_with(
        "SELECT * FROM Users WHERE UserId = :__QUERY_PARAM_0__",
        params={"user_id": "105 OR 1=1", "__QUERY_PARAM_0__": "105 OR 1=1"},
        database="db",
        boto3_session=mocker.ANY,
        s3_output=mocker.ANY,
        ctas_approach=mocker.ANY,
        paramstyle="named",
    )
