import copy

import pandas as pd
import pytest
import requests.exceptions
import responses
from pytest import fixture
from pytest_mock import MockerFixture

from tests.one_drive.fixtures import FAKE_LIBRARIES, FAKE_SHEET
from toucan_connectors.common import HttpError
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector
from toucan_connectors.one_drive.one_drive_connector import (
    NotFoundError,
    OneDriveConnector,
    OneDriveDataSource,
)

import_path = "toucan_connectors.one_drive.one_drive_connector"


@fixture
def con(secrets_keeper):
    secrets_keeper.save("test", {"access_token": "access_token"})
    return OneDriveConnector(
        name="test",
        auth_flow_id="test",
        client_id="CLIENT_ID",
        client_secret="CLIENT_SECRET",
        redirect_uri="REDIRECT_URI",
        secrets_keeper=secrets_keeper,
        scope="offline_access Files.Read",
    )


@fixture
def ds():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="test_sheet",
        range="A2:B3",
    )


@fixture
def ds_error_sheet_and_table():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        sheet="test_sheet",
        table="test_table",
    )


@fixture
def ds_error_range_and_table():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        range="A2:B3",
        table="test_table",
    )


@fixture
def ds_error_no_sheet_no_table():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
    )


@fixture
def ds_with_multiple_sheets():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="sheet1, sheet2",
        range="A2:B3",
    )


@fixture
def ds_without_range():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="test_sheet",
    )


@fixture
def ds_with_table():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        table="test_table",
    )


@fixture
def ds_with_multiple_tables():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        table="table1, table2",
    )


@fixture
def ds_with_site():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="test_sheet",
        range="A2:B3",
        site_url="company_name.sharepoint.com/sites/site_name",
        document_library="Documents",
    )


@fixture
def ds_with_site_with_filename_pattern():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        match=True,
        file=r"test/.*\.jpg",
        sheet="test_sheet, other_sheet",
        range="A2:B3",
        site_url="company_name.sharepoint.com/sites/site_name",
        document_library="Documents",
    )


@fixture
def ds_with_site_with_file_regex_pattern():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        match=True,
        file=r".*\.jpg",
        sheet="test_sheet, other_sheet",
        range="A2:B3",
        site_url="company_name.sharepoint.com/sites/site_name",
        document_library="Documents",
    )


@fixture
def ds_with_site_sheme():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="test_sheet",
        range="A2:B3",
        site_url="https://company_name.sharepoint.com/sites/site_name/",
        document_library="Documents",
    )


@fixture
def ds_with_site_without_range():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="test_sheet",
        site_url="company_name.sharepoint.com/sites/site_name",
        document_library="Documents",
    )


@fixture
def ds_with_dates():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="test_sheet",
        parse_dates=["col_datetime", "col_datetime_bis"],
    )


@fixture
def ds_error_with_dates():
    return OneDriveDataSource(
        name="test_name",
        domain="test_domain",
        file="test_file",
        sheet="test_sheet",
        parse_dates=["col_text"],
    )


@fixture
def remove_secrets(secrets_keeper, con):
    secrets_keeper.save("test", {"access_token": None})


def fake_sheet(*args, **kwargs):
    return copy.deepcopy(FAKE_SHEET)


def test_user_input(con, ds_error_sheet_and_table, ds_error_range_and_table, ds_error_no_sheet_no_table):
    """It should return an error when the user inputs are wrong"""
    with pytest.raises(ValueError) as e:
        con.get_df(ds_error_sheet_and_table)
    assert str(e.value) == "You cannot specifiy both sheets and tables"

    with pytest.raises(ValueError) as e:
        con.get_df(ds_error_range_and_table)
    assert str(e.value) == "You cannot specify a range for tables (tables is a kind of range)"

    with pytest.raises(ValueError) as e:
        con.get_df(ds_error_no_sheet_no_table)
    assert str(e.value) == "You must specify at least a sheet or a table"


def test_get_status_no_secrets(con, remove_secrets):
    """
    Check that the connection status is false when no secret is defined
    """
    assert con.get_status().status is False


def test_get_status_secrets_error(mocker, con):
    """
    Check that the connector status is false if the
    secret manager is not able to retrieve the access token
    """
    mocker.patch(f"{import_path}.OAuth2Connector.get_access_token", side_effect=Exception)
    assert con.get_status().status is False


def test_get_status_api_down(mocker, con):
    """
    Check that the connection status is false when the secret manager receives an httperror
    """
    mocker.patch.object(OneDriveConnector, "_get_access_token", side_effect=HttpError)
    assert con.get_status().status is False


def test_get_status_ok(mocker, con):
    """
    Check that we get the connector status set to True if
    the access token is correctly retrieved
    """
    mocker.patch.object(OneDriveConnector, "_get_access_token", return_value="i_am_a_token")
    assert con.get_status().status is True


def test_sheet_success(mocker, con, ds, ds_with_table):
    """It should return a dataframe"""
    run_fetch = mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    df = con.get_df(ds)

    assert run_fetch.call_count == 1
    assert df.shape == (2, 8)
    assert df.columns.tolist() == [
        "col_text",
        "col_int",
        "col_float",
        "col_money",
        "col_date",
        "col_datetime",
        "col_datetime_bis",
        "col_mixed_type",
    ]

    df = con.get_df(ds_with_table)

    assert run_fetch.call_count == 2
    assert df.shape == (2, 8)
    assert df.columns.tolist() == [
        "col_text",
        "col_int",
        "col_float",
        "col_money",
        "col_date",
        "col_datetime",
        "col_datetime_bis",
        "col_mixed_type",
    ]


def test_multiple_sheets_success(mocker, con, ds_with_multiple_sheets, ds_with_multiple_tables):
    """It should return a dataframe"""
    run_fetch = mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    df = con.get_df(ds_with_multiple_sheets)

    assert run_fetch.call_count == 2
    assert df.shape == (4, 9)
    assert df.columns.tolist() == [
        "col_text",
        "col_int",
        "col_float",
        "col_money",
        "col_date",
        "col_datetime",
        "col_datetime_bis",
        "col_mixed_type",
        "__sheetname__",
    ]

    df = con.get_df(ds_with_multiple_tables)

    assert run_fetch.call_count == 4
    assert df.shape == (4, 9)
    assert df.columns.tolist() == [
        "col_text",
        "col_int",
        "col_float",
        "col_money",
        "col_date",
        "col_datetime",
        "col_datetime_bis",
        "col_mixed_type",
        "__tablename__",
    ]


def test_multiple_files_sheets_success(mocker, con, ds_with_site_with_filename_pattern):
    """It should return a dataframe"""
    mocker.patch.object(OneDriveConnector, "_retrieve_files_path", return_value=["foo", "bar"])
    run_fetch = mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)
    mocker.patch.object(OneDriveConnector, "_get_site_id", return_value="1234")
    mocker.patch.object(OneDriveConnector, "_get_list_id", return_value="abcd")
    df = con.get_df(ds_with_site_with_filename_pattern)
    assert run_fetch.call_count == 4
    assert run_fetch.call_args_list[0][0][0] == (
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/foo:"
        "/workbook/worksheets/test_sheet/range(address='A2:B3')"
    )
    assert run_fetch.call_args_list[1][0][0] == (
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/bar:"
        "/workbook/worksheets/test_sheet/range(address='A2:B3')"
    )
    assert run_fetch.call_args_list[2][0][0] == (
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/foo:"
        "/workbook/worksheets/other_sheet/range(address='A2:B3')"
    )
    assert run_fetch.call_args_list[3][0][0] == (
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/bar:"
        "/workbook/worksheets/other_sheet/range(address='A2:B3')"
    )
    assert df.shape == (8, 9)
    assert df.columns.tolist() == [
        "col_text",
        "col_int",
        "col_float",
        "col_money",
        "col_date",
        "col_datetime",
        "col_datetime_bis",
        "col_mixed_type",
        "__sheetname__",
    ]


def test_sheets_without_date(mocker, con, ds):
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    df = con.get_df(ds)

    assert df.loc[0].values.tolist() == ["foo", 1, 1.1, 1, 44197, 44198, 44198.5017476852, "toto"]


def test_sheets_with_dates(mocker, con, ds_with_dates):
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    df = con.get_df(ds_with_dates)

    assert df.loc[0].values.tolist() == [
        "foo",
        1,
        1.1,
        1,
        44197,
        pd.Timestamp("2021-01-02 00:00:00"),
        pd.Timestamp("2021-01-02 12:02:31"),
        "toto",
    ]


def test_error_sheets_with_dates(mocker, con, ds_error_with_dates):
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    with pytest.raises(ValueError) as e:
        con.get_df(ds_error_with_dates)
        assert str(e.value) == "Cannot convert column 'col_text' to datetime"


def test_empty_sheet(mocker, con, ds):
    """It should an empty df when the sheet is empty"""
    mocker.patch.object(OneDriveConnector, "_run_fetch", return_value={})

    df = con.get_df(ds)

    assert df.empty


def test_url_with_range(mocker, con, ds):
    """It should format the url when a range is provided"""
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    url = con._format_url(ds, "test_sheet", ds.file)

    assert (
        url == "https://graph.microsoft.com/v1.0/me/drive"
        "/root:/test_file:/workbook/worksheets/test_sheet/range(address='A2:B3')"
    )


def test_url_without_range(mocker, con, ds_without_range):
    """It should format the url when no range is provided"""
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    url = con._format_url(ds_without_range, "test_sheet", ds_without_range.file)

    assert (
        url == "https://graph.microsoft.com/v1.0/me/drive/"
        "root:/test_file:/workbook/worksheets/test_sheet/usedRange(valuesOnly=true)"
    )


def test_url_with_table(mocker, con, ds_with_table):
    """It should format the url when a table is provided"""
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)

    url = con._format_url(ds_with_table, "test_table", ds_with_table.file)

    assert url == "https://graph.microsoft.com/v1.0/me/drive/root:/test_file:/workbook/tables/test_table/range"


def test_url_with_site_with_range(mocker, con, ds_with_site):
    """It should format the url when a site and a range are provided"""
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)
    mocker.patch.object(OneDriveConnector, "_get_site_id", return_value="1234")
    mocker.patch.object(OneDriveConnector, "_get_list_id", return_value="abcd")

    url = con._format_url(ds_with_site, "test_sheet", ds_with_site.file)

    assert (
        url == "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/"
        "root:/test_file:/workbook/worksheets"
        "/test_sheet/range(address='A2:B3')"
    )


def test_url_with_site_without_range(mocker, con, ds_with_site_without_range):
    """It should format the url when a range but no range is provided"""
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=fake_sheet)
    mocker.patch.object(OneDriveConnector, "_get_site_id", return_value="1234")
    mocker.patch.object(OneDriveConnector, "_get_list_id", return_value="abcd")

    url = con._format_url(ds_with_site_without_range, "test_sheet", ds_with_site_without_range.file)

    assert (
        url == "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/test_file:/workbook/worksheets"
        "/test_sheet/usedRange(valuesOnly=true)"
    )


def test_build_authorization_uri(con, mocker):
    """It should build the authorization url"""
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "client_id"
    mock_oauth2_connector.client_secret = "secret"
    con._oauth2_connector = mock_oauth2_connector
    con.build_authorization_url()

    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_tokens(con, mocker):
    """It should retrieve the tokens"""
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "client_id"
    mock_oauth2_connector.client_secret = "secret"
    con._oauth2_connector = mock_oauth2_connector
    con.retrieve_tokens("foo")

    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_get_access_token(con, mocker):
    """It should get the access token"""
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "client_id"
    mock_oauth2_connector.client_secret = "secret"
    con._oauth2_connector = mock_oauth2_connector
    con._get_access_token()

    mock_oauth2_connector.get_access_token.assert_called()


@responses.activate
def test_run_fetch(con: OneDriveConnector, mocker: MockerFixture):
    """It should run fetch"""
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "client_id"
    mock_oauth2_connector.client_secret = "secret"
    con._oauth2_connector = mock_oauth2_connector

    responses.add(responses.GET, "https://jsonplaceholder.typicode.com/posts", json=[])
    con._run_fetch("https://jsonplaceholder.typicode.com/posts")

    mock_oauth2_connector.get_access_token.assert_called()


@responses.activate
def test__retrieve_files_path(con, mocker, ds_with_site_with_filename_pattern):
    """Check that _retrieve_files returns a list of files"""
    responses.add(
        responses.GET,
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/test:/children",
        status=200,
        json={
            "value": [
                {"name": "myfile.jpg", "size": 2048, "file": {}},
                {"name": "Documents", "folder": {"childCount": 4}},
                {"name": "Photos", "folder": {"childCount": 203}},
                {"name": "my sheet(1).xlsx", "size": 197},
            ],
            "@odata.nextLink": "https://...",
        },
    )
    mocker.patch.object(OneDriveConnector, "_get_site_id", return_value="1234")
    mocker.patch.object(OneDriveConnector, "_get_list_id", return_value="abcd")
    assert con._retrieve_files_path(ds_with_site_with_filename_pattern) == ["test/myfile.jpg"]


@responses.activate
def test__retrieve_files(con, mocker, ds_with_site_with_file_regex_pattern):
    """Check that _retrieve_files returns a list of files"""
    responses.add(
        responses.GET,
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root/children",
        status=200,
        json={
            "value": [
                {"name": "myfile.jpg", "size": 2048, "file": {}},
                {"name": "Documents", "folder": {"childCount": 4}},
                {"name": "Photos", "folder": {"childCount": 203}},
                {"name": "my sheet(1).xlsx", "size": 197},
            ],
            "@odata.nextLink": "https://...",
        },
    )
    mocker.patch.object(OneDriveConnector, "_get_site_id", return_value="1234")
    mocker.patch.object(OneDriveConnector, "_get_list_id", return_value="abcd")
    assert con._retrieve_files_path(ds_with_site_with_file_regex_pattern) == ["/myfile.jpg"]


@responses.activate
def test__retrieve_files_empty(con, mocker, ds_with_site_with_filename_pattern):
    """Check that _retrieve_files returns a list of files"""
    responses.add(
        responses.GET,
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/test:/children",
        status=200,
        json={
            "value": [],
            "@odata.nextLink": "https://...",
        },
    )
    mocker.patch.object(OneDriveConnector, "_get_site_id", return_value="1234")
    mocker.patch.object(OneDriveConnector, "_get_list_id", return_value="abcd")
    with pytest.raises(NotFoundError):
        con._retrieve_files_path(ds_with_site_with_filename_pattern)


@responses.activate
def test_get_site_id(con, mocker, ds_with_site, ds_with_site_sheme):
    """It should return a site id from a site url (including or not the https:// prefix, ending or not with /)"""
    responses.add(
        responses.GET,
        "https://graph.microsoft.com/v1.0/sites/company_name.sharepoint.com:/sites/site_name",
        json={"id": 1},
        status=200,
    )

    id = con._get_site_id(ds_with_site)
    assert id == 1

    id = con._get_site_id(ds_with_site_sheme)
    assert id == 1


@responses.activate
def test_get_list_id(con, mocker, ds_with_site):
    """It should return a list id among the lists (when list name is equal to the document_library)"""
    responses.add(responses.GET, "https://graph.microsoft.com/v1.0/sites/1234/lists", json=FAKE_LIBRARIES)

    id = con._get_list_id(ds_with_site, "1234")
    assert id == "abcd"


@responses.activate
def test__run_fetch_failed(con, mocker, ds_with_site):
    """It should return nothing as run fetch failed because of something"""
    responses.add(
        responses.GET,
        "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root/children",
        status=200,
        json={
            "value": [
                {"name": "myfile.jpg", "size": 2048, "file": {}},
                {"name": "Documents", "folder": {"childCount": 4}},
                {"name": "Photos", "folder": {"childCount": 203}},
                {"name": "my sheet(1).xlsx", "size": 197},
            ],
            "@odata.nextLink": "https://...",
        },
    )
    mocker.patch.object(OneDriveConnector, "_get_site_id", return_value="1234")
    mocker.patch.object(OneDriveConnector, "_get_list_id", return_value="abcd")
    mocker.patch.object(OneDriveConnector, "_run_fetch", side_effect=requests.exceptions.HTTPError)
    df = con._retrieve_data(ds_with_site)
    assert df.empty


def test_instantiate_light_one_drive_connector():
    light_connector = OneDriveConnector(
        name="my one drive", auth_flow_id="uuid", scope="scope", client_id="client_id", tenant="tenant"
    )
    assert light_connector.auth_flow_id == "uuid"
    assert light_connector._oauth2_connector.secrets_keeper is None
