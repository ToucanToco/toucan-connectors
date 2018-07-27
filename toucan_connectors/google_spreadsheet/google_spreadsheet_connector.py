from typing import List

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import GoogleCredentials


class GoogleSpreadsheetDataSource(ToucanDataSource):
    spreadsheet_id: str
    sheetname: str = None


class GoogleSpreadsheetConnector(ToucanConnector):
    """
    For authentication, download an authentication file from console.developper.com
    and use the values here. This is an oauth2 credential file. For more information
    see this: http://gspread.readthedocs.io/en/latest/oauth2.html
    """
    type = 'GoogleSpreadsheet'
    data_source_model: GoogleSpreadsheetDataSource

    credentials: GoogleCredentials
    scope: List[str] = ['https://www.googleapis.com/auth/drive',
                        'https://www.googleapis.com/auth/spreadsheets',
                        'https://spreadsheets.google.com/feeds']

    def get_df(self, data_source):
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            self.credentials.dict(),
            self.scope
        )
        gc = gspread.authorize(credentials)

        sheets = gc.open_by_key(data_source.spreadsheet_id)
        sheetname = data_source.sheetname
        sheet = sheets.sheet1 if sheetname is None else sheets.worksheet(sheetname)
        records = sheet.get_all_records()
        df = pd.DataFrame(records)
        return df
