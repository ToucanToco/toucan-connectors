"""
Real-life responses of the Sheets API can be obtained in their documentation:
https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
"""

from os import path

from google_sheets.google_sheets_connector import GoogleSheetsConnector, GoogleSheetsDataSource
from googleapiclient.http import HttpMock
from pandas import DataFrame
from pytest_mock import MockFixture


def test_retrieve_data_no_sheet(mocker: MockFixture):
    mocker.patch(
        'google_sheets.google_sheets_connector.GoogleSheetsConnector._google_client_build_kwargs',
        return_value={
            'http': HttpMock(
                path.join(path.dirname(__file__), './sample-response.json'), {'status': '200'}
            )
        },
    )

    gsheet_connector = GoogleSheetsConnector(
        name='test_connector',
        retrieve_token=lambda _: 'test_access_token',
        auth_flow_id='test_auth_flow_id',
    )

    df = gsheet_connector.get_df(
        data_source=GoogleSheetsDataSource(
            name='test_connector', domain='test_domain', spreadsheet_id='test_spreadsheet_id'
        )
    )

    assert isinstance(df, DataFrame)
