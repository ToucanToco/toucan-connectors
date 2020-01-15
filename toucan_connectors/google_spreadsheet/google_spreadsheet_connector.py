from typing import List

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import Field

from toucan_connectors.google_credentials import GoogleCredentials
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class GoogleSpreadsheetDataSource(ToucanDataSource):
    spreadsheet_id: str = Field(
        ...,
        title='Spreadsheet ID',
        description='You can find this ID in the URL of your spreadsheet, '
        'just after the base path "https://docs.google.com/spreadsheets/d/"',
    )
    sheetname: str = Field(
        None, description='If not specified, the first sheet will be extracted by default'
    )
    skip_rows: int = Field(
        0,
        title='Number of rows to skip',
        description='If the first rows of your spreadsheet do not contain relevant data',
    )


class GoogleSpreadsheetConnector(ToucanConnector):
    """
    For authentication, download an authentication file from console.developper.com
    and use the values here. This is an oauth2 credential file. For more information
    see this: http://gspread.readthedocs.io/en/latest/oauth2.html
    """

    data_source_model: GoogleSpreadsheetDataSource

    credentials: GoogleCredentials = Field(
        ...,
        title='Google Credentials',
        description='For authentication, download an authentication file from your '
        '<a href="https://console.developers.google.com/apis/credentials">Google Console</a> '
        'and use the values here. This is an oauth2 credential file. For more information see this '
        '<a href="https://gspread.readthedocs.io/en/latest/oauth2.html">documentation</a>. '
        'You should use "service_account" credentials, which is the preferred type of credentials '
        'to use when authenticating on behalf of a service or application',
    )
    scope: List[str] = Field(
        [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://spreadsheets.google.com/feeds',
        ],
        description='OAuth 2.0 scopes define the level of access you need to '
        'request the Google APIs. For more information, see this '
        '<a href="https://developers.google.com/identity/protocols/googlescopes">documentation</a>',
    )

    def _retrieve_data(self, data_source):
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            self.credentials.dict(), self.scope
        )
        gc = gspread.authorize(credentials)

        sheets = gc.open_by_key(data_source.spreadsheet_id)
        sheetname = data_source.sheetname
        sheet = sheets.sheet1 if sheetname is None else sheets.worksheet(sheetname)
        starting_row = 1 + data_source.skip_rows
        records = sheet.get_all_records(head=starting_row)
        df = pd.DataFrame(records)
        return df
