from toucan_connectors.google_spreadsheet.google_spreadsheet_connector import (
    GoogleSpreadsheetConnector,
    GoogleSpreadsheetDataSource,
)

c = GoogleSpreadsheetConnector(
    name="test_name",
    credentials={
        "type": "service_account",
        "project_id": "test",
        "private_key_id": "test",
        "private_key": "test",
        "client_email": "test",
        "client_id": "test",
        "auth_uri": "https://test.com",
        "token_uri": "https://test.com",
        "auth_provider_x509_cert_url": "https://test.com",
        "client_x509_cert_url": "https://test.com",
    },
)

s = GoogleSpreadsheetDataSource(name="test_name", domain="test_domain", spreadsheet_id="test", load=False)


def test_spreadsheet(mocker):
    module = "toucan_connectors.google_spreadsheet.google_spreadsheet_connector"
    mocker.patch(f"{module}.get_google_oauth2_credentials")
    mocker.patch(
        f"{module}.gspread.authorize"
    ).return_value.open_by_key.return_value.sheet1.get_all_records.return_value = [
        {"a": 40},
        {"a": 1},
        {"a": 1},
    ]

    df = c.get_df(s)
    assert df.sum()["a"] == 42
