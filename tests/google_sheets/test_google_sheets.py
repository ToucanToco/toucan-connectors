"""
Real-life responses of the Sheets API can be obtained in their documentation:
https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
"""

from datetime import datetime
from os import path

import numpy as np
import pandas as pd
import pytest
from googleapiclient.http import HttpMock
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest_mock import MockFixture

from toucan_connectors.google_sheets.google_sheets_connector import (
    GoogleSheetsConnector,
    GoogleSheetsDataSource,
    GoogleSheetsInvalidConfiguration,
    parse_cell_value,
    serial_number_to_date,
)
from toucan_connectors.json_wrapper import JsonWrapper


def test_retrieve_data_with_dates(mocker: MockFixture):
    """
    It should retrieve the first sheet when no sheet has been indicated
    """
    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_build_kwargs",
        return_value={"http": HttpMock()},
    )

    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_request_kwargs",
        side_effect=[
            {
                # First call with sheet values
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./sheet-values-sample-data.json"),
                    {"status": "200"},
                )
            },
            {  # Second call with cell formats
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./sheet-formats-sample-data.json"),
                    {"status": "200"},
                )
            },
        ],
    )

    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=lambda _a, _b: "test_access_token",
        auth_id="test_auth_id",
    )

    df = gsheet_connector.get_df(
        data_source=GoogleSheetsDataSource(
            name="test_connector",
            domain="test_domain",
            spreadsheet_id="test_spreadsheet_id",
            sheet="sample data",
        )
    )

    assert_frame_equal(
        df,
        pd.DataFrame(
            columns=["label", "value", "date"],
            data=[
                ["A", 1, datetime(2022, 1, 4)],
                ["B", 2, datetime(2022, 1, 26)],
                ["C", 3, datetime(2021, 11, 30)],
            ],
        ),
    )


def test_retrieve_data_no_sheet(mocker: MockFixture):
    """
    It should retrieve the first sheet when no sheet has been indicated
    """
    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_build_kwargs",
        return_value={"http": HttpMock()},
    )

    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_request_kwargs",
        side_effect=[
            {  # First request to get sheet names
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./spreadsheet-sheets-properties.json"),
                    {"status": "200"},
                )
            },
            {
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./sheet-values-sample-data.json"),
                    {"status": "200"},
                )
            },
            {
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./sheet-formats-sample-data.json"),
                    {"status": "200"},
                )
            },
        ],
    )

    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=lambda _a, _b: "test_access_token",
        auth_id="test_auth_id",
    )

    df = gsheet_connector.get_df(
        data_source=GoogleSheetsDataSource(
            name="test_connector", domain="test_domain", spreadsheet_id="test_spreadsheet_id"
        )
    )

    assert isinstance(df, DataFrame)
    assert df.shape == (3, 3)


def test_retrieve_data_header_row(mocker: MockFixture):
    """
    It should use the provided header row for column names, and discard the others before
    """
    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_build_kwargs",
        return_value={"http": HttpMock()},
    )

    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_request_kwargs",
        side_effect=[
            {
                # First call with sheet values
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./sheet-values-animals.json"),
                    {"status": "200"},
                )
            },
            {  # Second call with cell formats
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./sheet-formats-animals.json"),
                    {"status": "200"},
                )
            },
        ],
    )

    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=lambda _a, _b: "test_access_token",
        auth_id="test_auth_id",
    )

    df = gsheet_connector.get_df(
        data_source=GoogleSheetsDataSource(
            name="test_connector",
            domain="test_domain",
            spreadsheet_id="test_spreadsheet_id",
            sheet="animals",
            header_row=1,
        )
    )

    assert_frame_equal(
        df,
        pd.DataFrame(
            columns=["animal", "lives"],
            data=[["cat", 7], ["elephant", 1], ["mouse", 0], ["vampire", None]],
        ),
    )


def test_get_status_no_secrets():
    """
    It should fail if no secrets are provided
    """
    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=lambda _a, _b: None,
        auth_id="test_auth_id",
    )
    assert gsheet_connector.get_status().status is False


def test_get_status_secrets_error():
    """
    It should fail if secrets can't be retrieved
    """

    def failing_retrieve_token(_a, _b):
        raise

    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=failing_retrieve_token,
        auth_id="test_auth_id",
    )
    assert gsheet_connector.get_status().status is False


def test_get_status_success(mocker: MockFixture):
    """
    It should be OK and indicate the email of the authenticated user
    """
    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_request_kwargs",
        return_value={
            "http": HttpMock(
                path.join(path.dirname(__file__), "./user-infos.json"),
                {"status": "200"},
            )
        },
    )

    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=lambda _a, _b: "access_token",
        auth_id="test_auth_id",
    )
    connector_status = gsheet_connector.get_status()
    assert connector_status.status is True
    assert "mewto@toucantoco.com" in connector_status.message


def test_get_status_api_down(mocker):
    """
    It should fail if the third-party api is down.
    """

    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_request_kwargs",
        return_value={
            "http": HttpMock(
                path.join(path.dirname(__file__), "./user-infos.json"),  # the content will not be read
                {"status": "400"},
            )
        },
    )

    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=lambda _a, _b: "access_token",
        auth_id="test_auth_id",
    )
    connector_status = gsheet_connector.get_status()
    assert connector_status.status is False


def test_schema_fields_order():
    schema_props_keys = list(JsonWrapper.loads(GoogleSheetsDataSource.schema_json())["properties"].keys())
    assert schema_props_keys[0] == "domain"
    assert schema_props_keys[1] == "spreadsheet_id"
    assert schema_props_keys[2] == "sheet"


def test_get_form(mocker):
    """It should return a list of spreadsheet titles."""
    mocker.patch(
        "toucan_connectors.google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_request_kwargs",
        side_effect=[
            {  # First request to get sheet names
                "http": HttpMock(
                    path.join(path.dirname(__file__), "./spreadsheet-sheets-properties.json"),
                    {"status": "200"},
                )
            },
        ],
    )

    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        retrieve_token=lambda _a, _b: "test_access_token",
        auth_id="test_auth_id",
    )

    data_source = GoogleSheetsDataSource(
        name="test_datasource",
        domain="test_domain",
        spreadsheet_id="test_spreadsheet_id",
        sheet="sample data",
    )

    schema = data_source.get_form(
        connector=gsheet_connector,
        current_config={"spreadsheet_id": "test_spreadsheet_id"},
    )
    expected_results = ["sample data", "animals"]
    assert schema["$defs"]["sheet"]["enum"] == expected_results


def test_numeric_dateformat_():
    value = 44303  # Example serial number representing a date
    format_ = {"numberFormat": {"type": "DATE"}}
    result = parse_cell_value(value, format_)
    assert result == serial_number_to_date(value)


def test_empty_string():
    value = ""
    format_ = {}
    result = parse_cell_value(value, format_)
    assert np.isnan(result)


def test_other_types():
    value = "hello"
    format_ = {"numberFormat": {"type": "DATE"}}
    result = parse_cell_value(value, format_)
    assert result == value

    value = 123
    format_ = {}
    result = parse_cell_value(value, format_)
    assert result == value


def test_default_format():
    value = 44303  # Example serial number representing a date
    result = parse_cell_value(value)
    assert result == value


def test_can_instantiate_without_retrieve_token_callback():
    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        auth_id="test_auth_id",
    )
    assert gsheet_connector


def test_raise_when_trying_to_retrieve_token_if_callable_missing():
    gsheet_connector = GoogleSheetsConnector(
        name="test_connector",
        auth_id="test_auth_id",
    )
    with pytest.raises(GoogleSheetsInvalidConfiguration):
        gsheet_connector.get_status()
