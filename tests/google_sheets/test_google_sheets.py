"""
Real-life responses of the Sheets API can be obtained in their documentation:
https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
"""
from datetime import datetime
from os import path

import pandas as pd
from google_sheets.google_sheets_connector import GoogleSheetsConnector, GoogleSheetsDataSource
from googleapiclient.http import HttpMock
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest_mock import MockFixture


def test_retrieve_data_with_dates(mocker: MockFixture):
    """
    It should retrieve the first sheet when no sheet has been indicated
    """
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
            name='test_connector',
            domain='test_domain',
            spreadsheet_id='test_spreadsheet_id',
            sheet='sample data',
        )
    )

    assert_frame_equal(
        df,
        pd.DataFrame(
            columns=['label', 'value', 'date'],
            data=[
                ['A', 1, datetime.fromisoformat('2022-01-04')],
                ['B', 2, datetime.fromisoformat('2022-01-26')],
                ['C', 3, datetime.fromisoformat('2021-11-30')],
            ],
        ),
    )


def test_retrieve_data_no_sheet(mocker: MockFixture):
    """
    It should retrieve the first sheet when no sheet has been indicated
    """
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
    assert df.shape == (3, 3)


def test_retrieve_data_header_row(mocker: MockFixture):
    """
    It should use the provided header row for column names, and discard the others before
    """
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
            name='test_connector',
            domain='test_domain',
            spreadsheet_id='test_spreadsheet_id',
            sheet='animals',
            header_row=1,
        )
    )

    assert_frame_equal(
        df,
        pd.DataFrame(
            columns=['animal', 'lives'],
            data=[['cat', 7], ['elephant', 1], ['mouse', 0], ['vampire', None]],
        ),
    )
