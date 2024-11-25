import tempfile
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Any

import openpyxl
import pandas as pd
import pytest
from dateutil.tz import tzutc
from pytest_mock import MockFixture

from toucan_connectors.s3.s3_connector import S3Connector, S3DataSource
from toucan_connectors.toucan_connector import ConnectorStatus


@pytest.fixture
def raw_connector() -> S3Connector:
    return S3Connector(
        name="my_sts_s3_connector",
        bucket_name="my-s3-bucket",
        role_arn="my-role-arn",
        prefix="some/path",
        workspace_id="workspace-id",
        sts_access_key_id="id",
        sts_secret_access_key="secret",
    )


@pytest.fixture
def connector(mocker: MockFixture, raw_connector: S3Connector) -> Generator[Any, Any, Any]:
    mocker.patch.object(
        raw_connector,
        "_get_assumed_sts_role",
        return_value={
            "Credentials": {
                "AccessKeyId": "test",
                "SecretAccessKey": "toto",
                "SessionToken": "tata",
            }
        },
    )
    yield raw_connector


@pytest.fixture
def sts_data_source_csv() -> Generator[Any, Any, Any]:
    yield S3DataSource(
        domain="test",
        name="test",
        file="my-file.csv",
        reader_kwargs={"preview_nrows": 2},
        fetcher_kwargs={},
    )


@pytest.fixture
def sts_data_source_xlsx() -> Generator[Any, Any, Any]:
    yield S3DataSource(
        domain="test",
        name="test",
        file="my-file.xlsx",
        reader_kwargs={"engine": "openpyxl"},
        fetcher_kwargs={},
    )


@pytest.fixture
def sts_data_source_regex() -> Generator[Any, Any, Any]:
    yield S3DataSource(
        domain="test",
        name="test",
        file="data[0-9]+\\.csv$",
        reader_kwargs={},
        fetcher_kwargs={},
    )


def test_get_status(mocker: MockFixture, connector: S3Connector) -> None:
    # Test case where get_sts_role returns without raising an exception
    expected_status = ConnectorStatus(status=True)
    actual_status = connector.get_status()
    assert actual_status == expected_status

    mocker.patch.object(connector, "_get_assumed_sts_role", side_effect=Exception("Error"))
    expected_status = ConnectorStatus(
        status=False,
        error="Cannot verify connection to S3 and/or AssumeRole failed : Error",
    )
    actual_status = connector.get_status()
    assert actual_status == expected_status


def test_forge_url(connector: S3Connector) -> None:
    assert connector._forge_url("key", "secret", "token", "file") == "s3://key:secret@my-s3-bucket/some/path/file"
    # with special characters, those needed to be urlencoded
    assert (
        connector._forge_url("k/e@y", "sec/re@special/t", "token1", "file")
        == "s3://k%2Fe%40y:sec%2Fre%40special%2Ft@my-s3-bucket/some/path/file"
    )
    # on prefix empty
    connector.prefix = ""
    assert connector._forge_url("key", "secret", "token3", "file") == "s3://key:secret@my-s3-bucket/file"
    connector.prefix = "tea/"
    assert connector._forge_url("key", "secret", "token", "fileC") == "s3://key:secret@my-s3-bucket/tea/fileC"
    connector.prefix = "/tea/secondo"
    assert connector._forge_url("key", "secret", "token", "fileB") == "s3://key:secret@my-s3-bucket/tea/secondo/fileB"
    connector.prefix = "///tea/secondo/tertio////"
    assert (
        connector._forge_url("key", "secret", "token", "fileA")
        == "s3://key:secret@my-s3-bucket/tea/secondo/tertio/fileA"
    )
    connector.prefix = "tea"
    assert connector._forge_url("key", "secret", "token", "/fileZ") == "s3://key:secret@my-s3-bucket/tea/fileZ"


def test_validate_external_id(mocker: MockFixture) -> None:
    # workspace_id should override external_id
    assert (
        S3Connector(
            name="my_sts_s3_connector",
            bucket_name="my-s3-bucket",
            role_arn="my-role-arn",
            prefix="some/path",
            workspace_id="a",
            external_id="b",
        ).external_id
        == "a"
    )


def test_retrieve_data_with_limit_offset(
    mocker: MockFixture,
    connector: S3Connector,
    sts_data_source_csv: S3DataSource,
    sts_data_source_xlsx: S3DataSource,
) -> None:
    # We mock s3_open()
    mock_s3_open = mocker.patch("peakina.io.s3.s3_utils.s3_open")
    mock_s3_open_retries = mocker.patch("peakina.io.s3.s3_utils._s3_open_file_with_retries")
    boto3_session = mocker.patch("toucan_connectors.s3.s3_connector.boto3.Session")
    boto3_session.return_value.client.return_value.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "my-file.csv"}, {"Key": "my-file.xlsx"}]}
    ]

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp_excel_file:
        with tempfile.NamedTemporaryFile(suffix=".csv") as temp_csv_file:
            ### --- for excel --- ###
            excel_df = pd.DataFrame({"X": [1, 2, 3, 4], "Y": [5, 6, 7, 8], "Z": [9, 10, 11, 12]})
            excel_df.to_excel(temp_excel_file.name, engine="openpyxl", index=False)

            mocker.patch("tempfile.NamedTemporaryFile", return_value=temp_excel_file)
            expected_return = excel_df.to_string()
            mock_s3_open_retries.return_value.read.return_value = expected_return.encode("utf-8")
            # s3_open side_effect
            mock_s3_open.side_effect = [
                temp_excel_file.read(),
                openpyxl.load_workbook(temp_excel_file.name),
            ]

            result = connector._retrieve_data(sts_data_source_xlsx, offset=2, limit=1)

            # assert that result is a DataFrame and has the expected values
            assert isinstance(result, pd.DataFrame)
            expected_result = pd.DataFrame({"X": [3], "Y": [7], "Z": [11], "__filename__": "my-file.xlsx"})
            assert result.equals(expected_result)

            ### --- for csv --- ###
            csv_df = pd.DataFrame({"A": [1, 2, 3, 4], "B": [5, 6, 7, 8], "C": [9, 10, 11, 12]})
            csv_df.to_csv(temp_csv_file.name, index=False)

            mocker.patch("tempfile.NamedTemporaryFile", return_value=temp_csv_file)
            expected_return = csv_df.to_csv(index=False, sep=",") or ""
            mock_s3_open_retries.return_value.read.return_value = expected_return.encode("utf-8")
            # s3_open side_effect
            mock_s3_open.side_effect = [
                temp_csv_file.read().decode("utf-8"),
                pd.read_csv(temp_csv_file.name),
            ]

            result = connector._retrieve_data(sts_data_source_csv, offset=1, limit=2)
            # assert that result is a DataFrame and has the expected values
            assert isinstance(result, pd.DataFrame)
            expected_result = pd.DataFrame({"A": [2, 3], "B": [6, 7], "C": [10, 11], "__filename__": "my-file.csv"})
            assert result.equals(expected_result)


def test_retrieve_data_match_patterns(
    mocker: MockFixture, connector: S3Connector, sts_data_source_regex: S3DataSource
) -> None:
    connector._forge_url = mocker.Mock(return_value="s3://example.com/data.csv")

    boto3_session = mocker.patch("toucan_connectors.s3.s3_connector.boto3.Session")
    boto3_session.return_value.client.return_value.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "data/file1.txt"},
                {"Key": "data1.csv"},
                {"Key": "data123.csv"},
                {"Key": "data/subfolder/file3.txt"},
                {"Key": "data/subfolder/data2.csv"},
            ]
        }
    ]
    peakina_datasource = mocker.patch("toucan_connectors.s3.s3_connector.DataSource")
    peakina_datasource.return_value.get_df.return_value = pd.DataFrame()

    # Invoke the _retrieve_data method
    _ = connector._retrieve_data(sts_data_source_regex)

    # Assertions
    connector._forge_url.assert_called()
    # the url forger was called 2 times
    assert connector._forge_url.call_count == 2
    # for data1.csv and data123.csv because they match the regex
    # 'data[0-9]+\.csv$'
    assert connector._forge_url.call_args_list[0][1]["file"] == "data1.csv"
    assert connector._forge_url.call_args_list[1][1]["file"] == "data123.csv"


def test_get_assumed_sts_role_cached(mocker: MockFixture, raw_connector: S3Connector) -> None:
    """should cache assume role"""
    boto3_client = mocker.patch("toucan_connectors.s3.s3_connector.boto3.client")
    sts_client = boto3_client()
    sts_client.assume_role.return_value = {
        "Credentials": {"Expiration": datetime.utcnow().replace(tzinfo=tzutc()) + timedelta(hours=1)}
    }
    raw_connector._get_assumed_sts_role()
    raw_connector._get_assumed_sts_role()
    assert sts_client.assume_role.call_count == 1


def test_get_assumed_sts_role_expired(mocker: MockFixture, raw_connector: S3Connector) -> None:
    """should invalidate cache and re-assume role when expired"""
    boto3_client = mocker.patch("toucan_connectors.s3.s3_connector.boto3.client")
    sts_client = boto3_client()
    sts_client.assume_role.return_value = {
        "Test": "OK",
        "Credentials": {"Expiration": datetime.utcnow().replace(tzinfo=tzutc()) + timedelta(hours=-1)},
    }
    raw_connector._get_assumed_sts_role()
    assert sts_client.assume_role.call_count == 2
