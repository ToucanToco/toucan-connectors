from typing import List

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class GoogleCredentials(BaseModel):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str


class GoogleSpreadsheetDataSource(ToucanDataSource):
    spreadsheet_id: str
    sheetname: str = None


class GoogleSpreadsheetConnector(ToucanConnector):
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
