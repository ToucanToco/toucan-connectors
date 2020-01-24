import os

from toucan_connectors.google_sheets.google_sheets_connector import (
    GoogleSheetsConnector,
    GoogleSheetsDataSource,
)

con = GoogleSheetsConnector(name='test_name', bearer_auth_id='qweqwe-1111-1111-1111-qweqweqwe')

ds = GoogleSheetsDataSource(
    name='test_name',
    domain='test_domain',
    sheet='Constants',
    spreadsheet_id='1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU',
)


def test_spreadsheet(mocker):
    mocker.patch.dict(os.environ, {'BEARER_API_KEY': 'my_bearer_api_key'})
    bearer_mock = mocker.patch('toucan_connectors.toucan_connector.Bearer')
    integration_mock = bearer_mock.return_value.integration
    auth_mock = integration_mock.return_value.auth
    get_mock = auth_mock.return_value.get
    get_mock.return_value.json.return_value = {
        'metadata': '...',
        'values': [['country', 'city'], ['France', 'Paris'], ['England', 'London']],
    }

    df = con.get_df(ds)
    bearer_mock.assert_called_once_with('my_bearer_api_key')
    integration_mock.assert_called_once_with('google_sheets')
    auth_mock.assert_called_once_with('qweqwe-1111-1111-1111-qweqweqwe')
    get_mock.assert_called_once_with(
        '1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU/values/Constants', query=None
    )
    assert df.shape == (3, 2)
    assert df.columns.tolist() == [0, 1]

    ds.header_row = 1
    df = con.get_df(ds)
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['country', 'city']

    assert con.schema()['properties']['bearer_auth_id'] == {
        'title': 'Bearer Auth Id',
        'type': 'string',
    }
    assert con.schema()['properties']['bearer_integration'] == {
        'default': 'google_sheets',
        'title': 'Bearer Integration',
        'type': 'string',
    }
    assert con.schema()['required'] == ['name', 'bearer_auth_id']
