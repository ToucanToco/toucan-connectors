from toucan_connectors.google_spreadsheet import (
    GoogleSpreadsheetDataSource, GoogleSpreadsheetConnector
)


c = GoogleSpreadsheetConnector(
    name='test_name',
    credentials={
        'type': 'service_account',
        'project_id': 'test',
        'private_key_id': 'test',
        'private_key': 'test',
        'client_email': 'test',
        'client_id': 'test',
        'auth_uri': 'test',
        'token_uri': 'test',
        'auth_provider_x509_cert_url': 'test',
        'client_x509_cert_url': 'test',
    }
)

s = GoogleSpreadsheetDataSource(
    name='test_name',
    domain='test_domain',
    spreadsheet_id='test'
)


def test_spreadsheet(mocker):
    module = 'toucan_connectors.google_spreadsheet.google_spreadsheet_connector'
    mocker.patch(f'{module}.ServiceAccountCredentials.from_json_keyfile_dict')
    mocker.patch(f'{module}.gspread.authorize')\
        .return_value\
        .open_by_key\
        .return_value\
        .sheet1\
        .get_all_records\
        .return_value = [{"a": 40}, {"a": 1}, {"a": 1}]

    df = c.get_df(s)
    assert df.sum()["a"] == 42
