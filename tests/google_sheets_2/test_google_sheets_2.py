import datetime
from unittest.mock import Mock

import pytest
from pytest import fixture
from pytest_mock import MockerFixture

from toucan_connectors.common import HttpError
from toucan_connectors.google_sheets_2.google_sheets_2_connector import (
    GoogleSheets2Connector,
    GoogleSheets2DataSource,
    NoCredentialsError,
)
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector

import_path = "toucan_connectors.google_sheets_2.google_sheets_2_connector"


@fixture
def con(secrets_keeper):
    secrets_keeper.save("test", {"access_token": "access_token"})
    return GoogleSheets2Connector(
        name="test",
        auth_flow_id="test",
        client_id="CLIENT_ID",
        client_secret="CLIENT_SECRET",
        redirect_uri="REDIRECT_URI",
        secrets_keeper=secrets_keeper,
    )


@fixture
def remove_secrets(secrets_keeper, con):
    secrets_keeper.save("test", {"access_token": None})


@fixture
def ds():
    return GoogleSheets2DataSource(
        name="test_name",
        domain="test_domain",
        sheet="Constants",
        spreadsheet_id="1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU",
    )


@fixture
def ds_without_sheet():
    return GoogleSheets2DataSource(
        name="test_name",
        domain="test_domain",
        spreadsheet_id="1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU",
    )


FAKE_SHEET = {
    "metadata": "...",
    "values": [["country", "city"], ["France", "Paris"], ["England", "London"]],
}


@pytest.mark.asyncio
async def test_authentified_fetch(mocker, con):
    """It should return a result from fetch if all is ok."""
    mocker.patch(f"{import_path}.fetch", return_value=FAKE_SHEET)

    result = await con._fetch("/foo")

    assert result == FAKE_SHEET


FAKE_SHEET_LIST_RESPONSE = {
    "sheets": [
        {"properties": {"title": "Foo"}},
        {"properties": {"title": "Bar"}},
        {"properties": {"title": "Baz"}},
    ]
}


def get_columns_in_schema(schema):
    """Pydantic generates schema slightly differently in python <=3.7 and in python 3.8"""
    try:
        if defs := schema.get("$defs") or schema.get("definitions"):
            return defs["sheet"]["enum"]
        else:
            return schema["properties"]["sheet"]["enum"]
    except KeyError:
        return None


def test_get_form_with_secrets(mocker, con, ds):
    """It should return a list of spreadsheet titles."""
    mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value=FAKE_SHEET_LIST_RESPONSE)

    result = ds.get_form(
        connector=con,
        current_config={"spreadsheet_id": "1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU"},
    )
    expected_results = ["Foo", "Bar", "Baz"]
    assert get_columns_in_schema(result) == expected_results


def test_get_form_no_secrets(mocker, con, ds, remove_secrets):
    """It should return no spreadsheet titles."""
    mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value=Exception)
    result = ds.get_form(
        connector=con,
        current_config={"spreadsheet_id": "1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU"},
    )
    assert not get_columns_in_schema(result)


def test_spreadsheet_success(mocker, con, ds):
    """It should return a spreadsheet."""
    mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value=FAKE_SHEET)

    df = con.get_df(ds)

    assert df.shape == (2, 2)
    assert df.columns.tolist() == ["country", "city"]

    ds.header_row = 1
    df = con.get_df(ds)
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["France", "Paris"]


def test_spreadsheet_no_secrets(mocker, con, ds, remove_secrets):
    """It should raise an exception if there are no secrets returned or any document in database."""
    mocker.patch.object(GoogleSheets2Connector, "_fetch", return_value=FAKE_SHEET)
    with pytest.raises(NoCredentialsError) as err:
        con.get_df(ds)

    assert str(err.value) == "No credentials"

    with pytest.raises(NoCredentialsError):
        con.get_df(ds)


def test_set_columns(mocker, con, ds):
    """It should return a well-formed column set."""
    fake_results = {
        "metadata": "...",
        "values": [["Animateur", "", "", "Week"], ["pika", "", "a", "W1"], ["bulbi", "", "", "W2"]],
    }
    mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value=fake_results)

    df = con.get_df(ds)
    assert df.to_dict() == {
        "Animateur": {1: "pika", 2: "bulbi"},
        "1": {1: "", 2: ""},
        "2": {1: "a", 2: ""},
        "Week": {1: "W1", 2: "W2"},
    }


def test__run_fetch(mocker, con):
    """It should return a result from loops if all is ok."""
    mocker.patch.object(GoogleSheets2Connector, "_fetch", return_value=FAKE_SHEET)

    result = con._run_fetch("/fudge")

    assert result == FAKE_SHEET


def test_spreadsheet_without_sheet(mocker, con, ds_without_sheet):
    """
    It should retrieve the first sheet of the spreadsheet if no sheet has been indicated
    """

    def mock_api_responses(uri: str):
        if "/Foo" in uri:
            return FAKE_SHEET
        else:
            return FAKE_SHEET_LIST_RESPONSE

    fetch_mock: Mock = mocker.patch.object(GoogleSheets2Connector, "_run_fetch", side_effect=mock_api_responses)
    df = con.get_df(ds_without_sheet)

    assert fetch_mock.call_count == 2
    assert (
        fetch_mock.call_args_list[0][0][0]
        == "https://sheets.googleapis.com/v4/spreadsheets/1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU"
    )
    assert (
        fetch_mock.call_args_list[1][0][0]
        == "https://sheets.googleapis.com/v4/spreadsheets/1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU/values/Foo?valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING"
    )

    assert df.shape == (2, 2)
    assert df.columns.tolist() == ["country", "city"]


def test_get_status_no_secrets(con, remove_secrets):
    """
    It should fail if no secrets are provided
    """
    assert con.get_status().status is False


def test_get_status_secrets_error(mocker, con):
    """
    It should fail if secrets can't be retrieved
    """
    mocker.patch(f"{import_path}.OAuth2Connector.get_access_token", side_effect=Exception)
    assert con.get_status().status is False


def test_get_status_success(mocker, con):
    """
    It should fail if no secrets are provided.
    """
    fetch_mock: Mock = mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value={"email": "foo@bar.baz"})

    connector_status = con.get_status()
    assert connector_status.status is True
    assert "foo@bar.baz" in connector_status.message

    fetch_mock.assert_called_once_with("https://www.googleapis.com/oauth2/v2/userinfo?alt=json")


def test_get_status_api_down(mocker, con):
    """
    It should fail if the third-party api is down.
    """
    mocker.patch.object(GoogleSheets2Connector, "_run_fetch", side_effect=HttpError)

    assert con.get_status().status is False


def test_get_decimal_separator(mocker, con, ds):
    """
    It should returns number data in float type
    """
    fake_results = {"metadata": "...", "values": [["Number"], [1.3], [1.2]]}
    mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value=fake_results)
    df = con.get_df(ds)
    assert df.to_dict() == {"Number": {1: 1.3, 2: 1.2}}


def test_delegate_oauth2_methods(mocker, con):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    con._oauth2_connector = mock_oauth2_connector
    con.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()
    con.retrieve_tokens("toto")
    mock_oauth2_connector.retrieve_tokens.assert_called_with("toto")


def test_get_slice(mocker, con, ds):
    """It should return a slice of spreadsheet"""
    run_fetch_mock = mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value=FAKE_SHEET)

    ds = con.get_slice(ds, limit=2)
    assert "!1:3" in run_fetch_mock.call_args_list[0][0][0]
    assert ds.df.shape == (2, 2)


def test_get_slice_no_limit(mocker: MockerFixture, con: GoogleSheets2Connector, ds: GoogleSheets2DataSource):
    """It should return a slice of spreadsheet"""
    mocker.patch.object(GoogleSheets2Connector, "_run_fetch", return_value=FAKE_SHEET)

    slice = con.get_slice(ds, limit=None)

    assert slice.df.shape == (2, 2)


def test_schema_fields_order(con, ds):
    schema_props_keys = list(JsonWrapper.loads(GoogleSheets2DataSource.schema_json())["properties"].keys())
    assert schema_props_keys[0] == "domain"
    assert schema_props_keys[1] == "spreadsheet_id"
    assert schema_props_keys[2] == "sheet"


def test_parse_datetime(mocker, con):
    ds = GoogleSheets2DataSource(
        name="test_name",
        domain="test_domain",
        sheet="Constants",
        spreadsheet_id="1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU",
        parse_dates=["a_date"],
    )
    fake_result = {
        "metadata": "...",
        "values": [
            ["country", "city", "a_date"],
            ["France", "Paris", "2001-02-02"],
            ["England", "London", "2010-08-09"],
        ],
    }
    mocker.patch(f"{import_path}.fetch", return_value=fake_result)
    ds = con.get_slice(ds, limit=2)
    # Using pytz.utc rather than STL here because that's what pandas does
    assert ds.df["a_date"].iloc[0] == datetime.datetime(year=2001, month=2, day=2)
