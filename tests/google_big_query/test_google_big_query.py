import json
from collections.abc import Generator
from os import environ
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas
import pandas as pd
import pytest
import requests
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import ArrayQueryParameter, Client, ScalarQueryParameter
from google.cloud.bigquery.table import RowIterator
from google.cloud.exceptions import Unauthorized
from google.oauth2.service_account import Credentials
from pandas.testing import assert_frame_equal  # <-- for testing dataframes
from pydantic import ValidationError
from pytest_mock import MockerFixture, MockFixture

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.google_big_query.google_big_query_connector import (
    GoogleBigQueryConnector,
    GoogleBigQueryDataSource,
    GoogleClientCreationError,
    InvalidJWTToken,
    _define_query_param,
)
from toucan_connectors.google_credentials import GoogleCredentials, JWTCredentials

import_path = "toucan_connectors.google_big_query.google_big_query_connector"


@pytest.fixture
def fixture_credentials() -> GoogleCredentials:
    my_credentials = GoogleCredentials(
        type="my_type",
        project_id="my_project_id",
        private_key_id="my_private_key_id",
        private_key="my_private_key",
        client_email="my_client_email@email.com",
        client_id="my_client_id",
        auth_uri="https://accounts.google.com/o/oauth2/auth",
        token_uri="https://oauth2.googleapis.com/token",
        auth_provider_x509_cert_url="https://www.googleapis.com/oauth2/v1/certs",
        client_x509_cert_url="https://www.googleapis.com/robot/v1/metadata/x509/pika.com",
    )
    return my_credentials


@pytest.fixture
def jwt_fixture_credentials() -> JWTCredentials:
    my_credentials = JWTCredentials(
        project_id="THE_JWT_project_id",
        jwt_token="valid-jwt",
    )
    return my_credentials


@pytest.fixture
def gbq_connector_with_jwt(jwt_fixture_credentials: JWTCredentials) -> GoogleBigQueryConnector:
    # those should and can be None
    return GoogleBigQueryConnector(
        name="woups",
        scopes=["https://www.googleapis.com/auth/bigquery"],
        jwt_credentials=jwt_fixture_credentials,
    )


@pytest.fixture
def fixture_scope():
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
    ]
    return scopes


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        ("test", ScalarQueryParameter("test_param", "STRING", "test")),
        (0, ScalarQueryParameter("test_param", "INT64", 0)),
        (0.0, ScalarQueryParameter("test_param", "FLOAT64", 0.0)),
        (True, ScalarQueryParameter("test_param", "BOOL", True)),
        ([], ArrayQueryParameter("test_param", "STRING", [])),
        (["hi"], ArrayQueryParameter("test_param", "STRING", ["hi"])),
        ([0], ArrayQueryParameter("test_param", "INT64", [0])),
        ([0.0, 2], ArrayQueryParameter("test_param", "FLOAT64", [0.0, 2])),
        ([True, False], ArrayQueryParameter("test_param", "BOOL", [True, False])),
    ],
)
def test__define_query_param(input_value, expected_output):
    assert _define_query_param("test_param", input_value) == expected_output


def test_prepare_query_parameters():
    query = "SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = {{test_str}} AND test2 = {{test_float}} LIMIT 10"  # noqa:E501
    new_query, parameters = GoogleBigQueryConnector._prepare_query_and_parameters(
        query,
        {
            "test_str": "tortank",
            "test_int": 1,
            "test_float": 0.0,
            "test_bool": True,
        },
    )
    assert (
        new_query
        == "SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = @__QUERY_PARAM_0__ AND test2 = @__QUERY_PARAM_1__ LIMIT 10"  # noqa:E501
    )
    assert len(parameters) == 2
    assert parameters[0] == ScalarQueryParameter("__QUERY_PARAM_0__", "STRING", "tortank")
    assert parameters[1] == ScalarQueryParameter("__QUERY_PARAM_1__", "FLOAT64", 0.0)


def test_prepare_parameters_spaces():
    query = "SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = {{ test_str }} AND test2 = {{ test_float }} LIMIT 10"  # noqa:E501
    new_query, parameters = GoogleBigQueryConnector._prepare_query_and_parameters(
        query,
        {
            "test_str": "tortank",
            "test_int": 1,
            "test_float": 0.0,
            "test_bool": True,
        },
    )
    assert (
        new_query
        == "SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = @__QUERY_PARAM_0__ AND test2 = @__QUERY_PARAM_1__ LIMIT 10"  # noqa:E501
    )
    assert len(parameters) == 2
    assert parameters[0] == ScalarQueryParameter("__QUERY_PARAM_0__", "STRING", "tortank")
    assert parameters[1] == ScalarQueryParameter("__QUERY_PARAM_1__", "FLOAT64", 0.0)


def test_prepare_parameters_empty():
    query = "SELECT stuff FROM `useful-citizen-322414.test.test`"
    new_query, parameters = GoogleBigQueryConnector._prepare_query_and_parameters(query, None)
    assert len(parameters) == 0


def test__http_is_present_as_attr(
    mocker: MockFixture,
    gbq_connector_with_jwt: GoogleBigQueryConnector,
) -> None:
    """we should have _http as arg to bigquery.Client when the jwt is provided in google-credentials"""
    mock_bigquery_client = mocker.patch("google.cloud.bigquery.Client")
    gbq_connector_with_jwt._get_bigquery_client()
    assert mock_bigquery_client.call_count == 1
    # we ensure that _http is inside the list of called args
    assert ["project", "_http"] == list(mock_bigquery_client.call_args[1].keys())


def test_http_connect(
    mocker: MockFixture,
    gbq_connector_with_jwt: GoogleBigQueryConnector,
) -> None:
    """we should call for _http_connect when the jwt is provided in google-credentials"""
    mock_http_connect = mocker.patch(f"{import_path}.GoogleBigQueryConnector._http_connect")
    gbq_connector_with_jwt._get_bigquery_client()
    assert mock_http_connect.call_count == 1
    assert ["http_session", "project_id"] == list(mock_http_connect.call_args[1].keys())


def test_http_connect_on_invalid_token(
    mocker: MockFixture,
    gbq_connector_with_jwt: GoogleBigQueryConnector,
) -> None:
    """we should have _http as arg to bigquery.Client when the jwt is provided in google-credentials"""
    mock_bigquery_client = mocker.patch("google.cloud.bigquery.Client")
    mock_bigquery_client.side_effect = Unauthorized("Error with the JWT token")

    # when the JWT is not valid
    with pytest.raises(InvalidJWTToken):
        gbq_connector_with_jwt._http_connect(requests.Session(), "some-project-id")

    # when falling back on normal creds :
    with pytest.raises(GoogleClientCreationError):
        gbq_connector_with_jwt._get_bigquery_client()


@pytest.fixture
def gbq_credentials() -> Any:
    raw_creds = environ["GOOGLE_BIG_QUERY_CREDENTIALS"]
    return json.loads(raw_creds)


@pytest.fixture
def gbq_connector(gbq_credentials: Any) -> GoogleBigQueryConnector:
    return GoogleBigQueryConnector(name="gqb-test-connector", credentials=gbq_credentials)


@pytest.fixture
def gbq_datasource() -> GoogleBigQueryDataSource:
    return GoogleBigQueryDataSource(name="coucou", query="SELECT 1 AS `my_col`;", domain="test-domain")


def test_get_df(gbq_connector: GoogleBigQueryConnector, gbq_datasource: GoogleBigQueryDataSource):
    result = gbq_connector.get_df(gbq_datasource)
    expected = pandas.DataFrame({"my_col": [1]})
    assert_frame_equal(expected, result)


def test_get_df_with_variables(gbq_connector: GoogleBigQueryConnector, gbq_datasource: GoogleBigQueryDataSource):
    gbq_datasource.parameters = {"name": "Superstrong beer"}
    gbq_datasource.query = "SELECT name, price_per_l FROM `beers`.`beers_tiny` WHERE name = {{name}};"
    result = gbq_connector.get_df(gbq_datasource)
    expected = pandas.DataFrame({"name": ["Superstrong beer"], "price_per_l": [0.16]})
    assert_frame_equal(expected, result)


def test_get_df_with_type_casts(gbq_connector: GoogleBigQueryConnector, gbq_datasource: GoogleBigQueryDataSource):
    gbq_datasource.parameters = {"name": "Superstrong beer"}
    gbq_datasource.query = """
    WITH with_new_cols AS (
        SELECT *,
            CASE WHEN nullable_name IS NULL THEN NULL ELSE 1 END AS `nullable_int` ,
            CASE WHEN nullable_name IS NULL THEN NULL ELSE 0.5 END AS `nullable_float`,
        FROM `beers`.`beers_tiny` WHERE name = {{name}})
    SELECT name, nullable_name, nullable_int, nullable_float FROM with_new_cols;
    """
    result = gbq_connector.get_df(gbq_datasource)

    expected = pandas.DataFrame(
        {
            "name": ["Superstrong beer"],
            "nullable_name": [None],
            # We should have correct dtypes, not "object"
            "nullable_int": pd.Series([None]).astype("Int64"),
            "nullable_float": pd.Series([None]).astype("float64"),
        }
    )
    assert_frame_equal(expected, result)
    assert result.dtypes["nullable_int"] == pd.Int64Dtype()
    assert result.dtypes["nullable_float"] == np.float64


@patch(
    "google.cloud.bigquery.table.RowIterator.to_dataframe",
    return_value=pandas.DataFrame({"a": [1, 1], "b": [2, 2]}),
)
@patch("google.cloud.bigquery.job.query.QueryJob.result", return_value=RowIterator)
@patch("google.cloud.bigquery.Client.query", side_effect=TypeError)
@patch("google.cloud.bigquery.Client", autospec=True)
def test_execute_error(client, execute, result, to_dataframe):
    with pytest.raises(TypeError):
        GoogleBigQueryConnector._execute_query(client, "SELECT 1 FROM my_table", [])


@patch(
    "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._get_google_credentials",
    return_value=Credentials,
)
@patch(
    "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._connect",
    return_value=Client,
)
@patch(
    "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._execute_query",
    return_value=pandas.DataFrame({"a": [1, 1], "b": [2, 2]}),
)
def test_retrieve_data(execute, connect, credentials, fixture_credentials):
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        credentials=fixture_credentials,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    datasource = GoogleBigQueryDataSource(
        name="MyGBQ",
        domain="wiki",
        query="SELECT * FROM bigquery-public-data:samples.wikipedia WHERE test = '{{key}}' LIMIT 1000",
        parameters={"key": "tortank"},
    )
    result = connector._retrieve_data(datasource)
    assert_frame_equal(pandas.DataFrame({"a": [1, 1], "b": [2, 2]}), result)


def test_get_model(mocker: MockFixture, fixture_credentials) -> None:
    class FakeResponse:
        def __init__(self) -> None: ...

        def to_dataframe(self) -> Generator[Any, Any, Any]:
            yield pd.DataFrame(
                [
                    {
                        "name": "coucou",
                        "schema": "foooo",
                        "database": "myproject",
                        "type": "BASE TABLE",
                        "column_name": "pingu",
                        "data_type": "STR",
                    },
                    {
                        "name": "coucou",
                        "schema": "foooo",
                        "database": "myproject",
                        "type": "BASE TABLE",
                        "column_name": "toto",
                        "data_type": "INT64",
                    },
                    {
                        "name": "coucou",
                        "schema": "foooo",
                        "database": "myproject",
                        "type": "BASE TABLE",
                        "column_name": "tante",
                        "data_type": "STR",
                    },
                    {
                        "name": "blabla",
                        "schema": "baarrrr",
                        "database": "myproject",
                        "type": "MATERIALIZED VIEW",
                        "column_name": "gogo",
                        "data_type": "STR",
                    },
                    {
                        "name": "blabla",
                        "schema": "baarrrr",
                        "database": "myproject",
                        "type": "MATERIALIZED VIEW",
                        "column_name": "gaga",
                        "data_type": "INT64",
                    },
                    {
                        "name": "blabla",
                        "schema": "baarrrr",
                        "database": "myproject",
                        "type": "MATERIALIZED VIEW",
                        "column_name": "gg",
                        "data_type": "STR",
                    },
                    {
                        "name": "tortuga",
                        "schema": "taar",
                        "database": "myproject",
                        "type": "VIEW",
                        "column_name": "hammer",
                        "data_type": "STR",
                    },
                    {
                        "name": "tortuga",
                        "schema": "taar",
                        "database": "myproject",
                        "type": "VIEW",
                        "column_name": "to",
                        "data_type": "INT64",
                    },
                    {
                        "name": "tortuga",
                        "schema": "taar",
                        "database": "myproject",
                        "type": "VIEW",
                        "column_name": "fall",
                        "data_type": "STR",
                    },
                ]
            )

    datasets = [
        mocker.MagicMock(dataset_id="foooo"),
        mocker.MagicMock(dataset_id="baarrrr"),
        mocker.MagicMock(dataset_id="taar"),
    ]

    mocker.patch.object(Client, "list_datasets", return_value=datasets)
    mocked_query = mocker.patch.object(Client, "query", return_value=FakeResponse())
    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._connect",
        return_value=Client,
    )

    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._fetch_query_results",
        return_value=FakeResponse().to_dataframe(),
    )

    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._get_google_credentials",
        return_value=Credentials,
    )
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        credentials=fixture_credentials,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    assert connector.get_model() == [
        {
            "name": "blabla",
            "schema": "baarrrr",
            "database": "myproject",
            "type": "view",
            "columns": [
                {"name": "gogo", "type": "str"},
                {"name": "gaga", "type": "int64"},
                {"name": "gg", "type": "str"},
            ],
        },
        {
            "name": "coucou",
            "schema": "foooo",
            "database": "myproject",
            "type": "table",
            "columns": [
                {"name": "pingu", "type": "str"},
                {"name": "toto", "type": "int64"},
                {"name": "tante", "type": "str"},
            ],
        },
        {
            "name": "tortuga",
            "schema": "taar",
            "database": "myproject",
            "type": "view",
            "columns": [
                {"name": "hammer", "type": "str"},
                {"name": "to", "type": "int64"},
                {"name": "fall", "type": "str"},
            ],
        },
    ]
    assert (
        mocked_query.call_args_list[0][0][0]
        == """
SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `foooo`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `foooo`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'

UNION ALL

SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `baarrrr`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `baarrrr`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'

UNION ALL

SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `taar`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `taar`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
"""
    )
    mocked_query.reset_mock()

    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._fetch_query_results",
        return_value=FakeResponse().to_dataframe(),
    )

    connector.get_model("some-db")
    assert (
        mocked_query.call_args_list[0][0][0]
        == """
SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `foooo`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `foooo`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
AND T.table_catalog = 'some-db'

UNION ALL

SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `baarrrr`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `baarrrr`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
AND T.table_catalog = 'some-db'

UNION ALL

SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `taar`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `taar`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
AND T.table_catalog = 'some-db'
"""
    )

    mocked_query.reset_mock()

    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._fetch_query_results",
        return_value=FakeResponse().to_dataframe(),
    )

    connector.get_model("some-db", "foooo")

    # since we specified only the foooo schema we should only get the query for
    # it
    assert (
        mocked_query.call_args_list[0][0][0]
        == """
SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `foooo`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `foooo`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
AND T.table_catalog = 'some-db'
"""
    )


def test_get_model_multi_location(mocker: MockFixture, fixture_credentials) -> None:
    fake_resp_1 = mocker.MagicMock()
    fake_resp_1.to_dataframe.return_value = pd.DataFrame(
        [
            {
                "name": "coucou",
                "schema": "foooo",
                "database": "myproject",
                "type": "BASE TABLE",
                "column_name": "pingu",
                "data_type": "STR",
            },
            {
                "name": "coucou",
                "schema": "foooo",
                "database": "myproject",
                "type": "BASE TABLE",
                "column_name": "toto",
                "data_type": "INT64",
            },
            {
                "name": "coucou",
                "schema": "foooo",
                "database": "myproject",
                "type": "BASE TABLE",
                "column_name": "tante",
                "data_type": "STR",
            },
        ]
    )
    fake_resp_2 = mocker.MagicMock()
    fake_resp_2.to_dataframe.return_value = pd.DataFrame(
        [
            {
                "name": "blabla",
                "schema": "baarrrr",
                "database": "myproject",
                "type": "MATERIALIZED VIEW",
                "column_name": "gogo",
                "data_type": "STR",
            },
        ]
    )
    datasets = [
        mocker.MagicMock(dataset_id="foooo"),
        mocker.MagicMock(dataset_id="baarrrr"),
    ]
    datasets_info = [
        mocker.MagicMock(dataset_id="foooo", location="Paris"),
        mocker.MagicMock(dataset_id="baarrrr", location="Toulouse"),
    ]

    mocker.patch.object(Client, "list_datasets", return_value=datasets)
    mocker.patch.object(Client, "get_dataset", side_effect=datasets_info)
    mocked_query = mocker.patch.object(Client, "query", side_effect=[NotFound("Oh no"), fake_resp_1, fake_resp_2])
    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._connect",
        return_value=Client,
    )

    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._get_google_credentials",
        return_value=Credentials,
    )
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        credentials=fixture_credentials,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    assert connector.get_model() == [
        {
            "name": "blabla",
            "schema": "baarrrr",
            "database": "myproject",
            "type": "view",
            "columns": [{"name": "gogo", "type": "str"}],
        },
        {
            "name": "coucou",
            "schema": "foooo",
            "database": "myproject",
            "type": "table",
            "columns": [
                {"name": "pingu", "type": "str"},
                {"name": "toto", "type": "int64"},
                {"name": "tante", "type": "str"},
            ],
        },
    ]
    assert (
        mocked_query.call_args_list[0][0][0]
        == """
SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `foooo`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `foooo`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'

UNION ALL

SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `baarrrr`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `baarrrr`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
"""
    )
    # No location should be specified in the happy path
    assert mocked_query.call_args_list[0][1] == {}
    assert (
        mocked_query.call_args_list[1][0][0]
        == """
SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `foooo`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `foooo`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
"""
    )
    # Next calls should specify the location
    assert mocked_query.call_args_list[1][1] == {"location": "Paris"}
    assert (
        mocked_query.call_args_list[2][0][0]
        == """
SELECT
    C.table_name AS name,
    C.table_schema AS schema,
    T.table_catalog AS database,
    T.table_type AS type,
    C.column_name,
    C.data_type
FROM
    `baarrrr`.INFORMATION_SCHEMA.COLUMNS C
    JOIN `baarrrr`.INFORMATION_SCHEMA.TABLES T
        ON C.table_name = T.table_name
WHERE
    IS_SYSTEM_DEFINED = 'NO'
    AND IS_HIDDEN = 'NO'
"""
    )
    # Next calls should specify the location
    assert mocked_query.call_args_list[2][1] == {"location": "Toulouse"}


def test_get_form(
    mocker: MockerFixture, fixture_credentials: GoogleCredentials, jwt_fixture_credentials: MockFixture
) -> None:
    def mock_available_schs():
        return ["ok", "test"]

    mocker.patch(
        "toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._available_schs",
        new=["ok", "test"],
    )

    assert (
        GoogleBigQueryDataSource(query=",", name="MyGBQ", domain="foo").get_form(
            GoogleBigQueryConnector(
                name="MyGBQ",
                credentials=fixture_credentials,
                scopes=[
                    "https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/drive",
                ],
            ),
            {},
        )["properties"]["database"]["default"]
        == "my_project_id"
    )

    assert (
        GoogleBigQueryDataSource(query=",", name="MyGBQ-WITH-JWT", domain="foo").get_form(
            GoogleBigQueryConnector(
                name="MyGBQ",
                jwt_credentials=jwt_fixture_credentials,
                scopes=[
                    "https://www.googleapis.com/auth/bigquery",
                ],
            ),
            {},
        )["properties"]["database"]["default"]
        == "THE_JWT_project_id"
    )

    # Now let say the JWT is not set or bad, or the project_id is also bad or
    # not set, we need to fallback on project_id provided by the GoogleCreds
    assert (
        GoogleBigQueryDataSource(query=",", name="MyGBQ-WITH-JWT", domain="foo").get_form(
            GoogleBigQueryConnector(
                name="MyGBQ",
                credentials=GoogleCredentials(
                    type="my_type",
                    project_id="THE_GOOGLE_CREDS_project_id",
                    private_key_id="my_private_key_id",
                    private_key="my_private_key",
                    client_email="my_client_email@email.com",
                    client_id="my_client_id",
                    auth_uri="https://accounts.google.com/o/oauth2/auth",
                    token_uri="https://oauth2.googleapis.com/token",
                    auth_provider_x509_cert_url="https://www.googleapis.com/oauth2/v1/certs",
                    client_x509_cert_url="https://www.googleapis.com/robot/v1/metadata/x509/pika.com",
                ),
                jwt_credentials=JWTCredentials(
                    project_id="THE_JWT_project_id",
                    jwt_token="",  # the jwt is empty
                ),
                scopes=[
                    "https://www.googleapis.com/auth/bigquery",
                ],
            ),
            {},
        )["properties"]["database"]["default"]
        == "THE_GOOGLE_CREDS_project_id"  # because the jwt_token value is not good or missing
    )


@pytest.mark.parametrize(
    "input_query, expected_output",
    [
        # Test cases with double quotes (unchanged)
        ('SELECT "column1" FROM table', "SELECT `column1` FROM table"),
        ('"quoted text" inside query', "`quoted text` inside query"),
        # Test cases with '@__' and '__''
        ("@__param1__ in query", "@__param1__ in query"),  # No change for param1
        (
            '"quoted text"@__param2__',
            "`quoted text`@__param2__",
        ),  # Replace double quotes, keep param2 intact
        (
            "@__param3__ and @__param4__",
            "@__param3__ and @__param4__",
        ),  # No change for params 3 and 4
        # Test cases with escaped single quotes
        ("'single-quoted text'", "'single-quoted text'"),  # No change for single-quoted text
        (
            "'escaped \\' single quote'",
            "'escaped \\' single quote'",
        ),  # No change for escaped single quote
        ("'@__param7__'", "@__param7__"),  # Remove surrounding single quotes for param7
        (
            "'@__param8__' and '@__param9__'",
            "@__param8__ and @__param9__",
        ),  # Remove surrounding single quotes for params 8 and 9
        # Mixed cases
        (
            '@__param5__ "with quotes" @__param6__',
            "@__param5__ `with quotes` @__param6__",
        ),  # No change for params 5 and 6
    ],
)
def test_clean_query(input_query, expected_output):
    assert GoogleBigQueryConnector._clean_query(input_query) == expected_output


def test_optional_fields_validator_for_google_creds_or_jwt():
    # FOR GOOGLE CREDS AUTH
    incomplete_credentials = {
        "type": "service_account",
        "project_id": "your-project-id",
    }
    # We should have an error raised, because the whole GoogleCreds should be
    # filled
    with pytest.raises(ValidationError):
        _ = GoogleBigQueryConnector(name="something", credentials=incomplete_credentials)

    # with valid values set
    valid_credentials = {
        "type": "service_account",
        "project_id": "your-project-id",
        "private_key_id": "my_private_key_id",
        "private_key": "my_private_key",
        "client_email": "my_client_email@email.com",
        "client_id": "my_client_id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/pika.com",
    }
    # no error raised
    _ = GoogleBigQueryConnector(name="something", credentials=valid_credentials)

    # FOR JWT GOOGLE CREDS AUTH
    incomplete_jwt_credentials_ = {
        "project_id": "test",
    }
    # We have an error, because the jwt_token is missing
    with pytest.raises(ValidationError) as _:
        _ = GoogleBigQueryConnector(name="something", jwt_credentials=incomplete_jwt_credentials_)

    # with valid values set
    valid_credentials = {
        "project_id": "your-project-id",
        "jwt_token": "valid-token",
    }
    # no error raised
    _ = GoogleBigQueryConnector(name="something", jwt_credentials=valid_credentials)


def test_get_project_id(fixture_credentials: GoogleCredentials) -> None:
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        credentials=fixture_credentials,
        jwt_credentials=JWTCredentials(jwt_token="token", project_id="123"),
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    # test get project id with jwt credentials
    assert connector._get_project_id() == "123"

    # test get project id with credentials
    connector.jwt_credentials = None
    connector.credentials = fixture_credentials
    connector.credentials.project_id = "456"
    assert connector._get_project_id() == "456"

    # On ProjectId missing...
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        credentials=None,
        jwt_credentials=JWTCredentials(jwt_token="", project_id=""),
        scopes=[],
    )
    with pytest.raises(GoogleClientCreationError):
        connector._get_project_id()


def test_get_status(mocker: MockerFixture, fixture_credentials: GoogleCredentials, sanitized_pem_key: str) -> None:
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        credentials=fixture_credentials,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    status = connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Credentials provided", True),
        ("Private key validity", False),
        ("Sample BigQuery job", False),
    ]
    assert status.error is not None
    assert "Could not deserialize key data" in status.error

    # Fix the key format
    connector.credentials.private_key = sanitized_pem_key

    # But now there is an error when creating the client
    connect_spy = mocker.spy(GoogleBigQueryConnector, "_connect")

    def connect_spy_fail(*args, **kwargs):
        raise Exception("Something happened while creating client")

    connect_spy.side_effect = connect_spy_fail
    status = connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Credentials provided", True),
        ("Private key validity", True),
        ("Sample BigQuery job", False),
    ]
    assert "Something happened while creating client" in status.error

    # Fix client creation but now the returned client cannot make a query
    def connect_spy_client_query_fail(*args, **kwargs):
        class FailingClient:
            def query(self, *args, **kwargs):
                raise Exception("Impossible to reach BigQuery")

        return FailingClient()

    connect_spy.side_effect = connect_spy_client_query_fail
    status = connector.get_status()
    assert status.status is False
    assert status.details == [
        ("Credentials provided", True),
        ("Private key validity", True),
        ("Sample BigQuery job", False),
    ]
    assert "Impossible to reach BigQuery" in status.error

    # Finally return a functioning client
    def connect_spy_ok(*args, **kwargs):
        class Client:
            def query(self, *args, **kwargs):
                return None

        return Client()

    connect_spy.side_effect = connect_spy_ok
    status = connector.get_status()
    assert status.status is True
    assert status.details == [
        ("Credentials provided", True),
        ("Private key validity", True),
        ("Sample BigQuery job", True),
    ]
    assert status.error is None


def test_get_status_with_jwt(mocker: MockerFixture, gbq_connector_with_jwt: GoogleBigQueryConnector) -> None:
    http_connect_mock = mocker.patch.object(gbq_connector_with_jwt, "_http_connect")
    status = gbq_connector_with_jwt.get_status()
    http_connect_mock.assert_called_once_with(http_session=mocker.ANY, project_id="THE_JWT_project_id")
    # no private key validity should appear here, as JWT auth was used
    assert status == ConnectorStatus(
        status=True, message=None, error=None, details=[("Credentials provided", True), ("Sample BigQuery job", True)]
    )


def test_get_status_no_creds() -> None:
    conn = GoogleBigQueryConnector(name="woups", scopes=["https://www.googleapis.com/auth/bigquery"])

    assert conn.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error="Either google credentials or a JWT token must be provided",
        details=[("Credentials provided", False), ("Sample BigQuery job", False)],
    )
