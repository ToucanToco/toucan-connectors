from datetime import datetime
from typing import Callable, List, Optional

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from pydantic import Field, PrivateAttr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class GoogleSheetsDataSource(ToucanDataSource):
    domain: str = Field(
        ...,
        title='dataset',
    )
    spreadsheet_id: str = Field(
        ...,
        title='ID of the spreadsheet',
        description='Can be found in your URL: '
        'https://docs.google.com/spreadsheets/d/<ID of the spreadsheet>/...',
    )
    sheet: Optional[str] = Field(
        None, title='Sheet title', description='Title of the desired sheet'
    )
    header_row: int = Field(
        0, title='Header row', description='Row of the header of the spreadsheet'
    )


class GoogleSheetsConnector(ToucanConnector):
    """
    This is a connector for [GoogleSheets](https://developers.google.com/sheets/api/reference/rest)

    It needs to be provided a retrieve_token method which should provide a valid OAuth2 access token.
    Not to be confused with the OAuth2 connector, which handles all the OAuth2 process byt itself!
    """

    data_source_model: GoogleSheetsDataSource

    _auth_flow = 'managed_oauth2'
    _retrieve_token: Callable[[str], str] = PrivateAttr()

    auth_flow_id: str

    def __init__(self, retrieve_token: Callable[[str], str], *args, **kwargs):
        super().__init__(**kwargs)
        self._retrieve_token = retrieve_token  # Could be async

    def _google_client_build_kwargs(self):
        # Override it for testing purposes
        access_token = self._retrieve_token(self.auth_flow_id)
        return {'credentials': Credentials(token=access_token)}

    def build_sheets_api(self):
        return build('sheets', 'v4', **self._google_client_build_kwargs())

    def list_sheets(self, spreadsheet_id: str) -> List[str]:
        """
        List available sheets
        """
        with self.build_sheets_api() as sheets_api:
            spreadsheet_data = (
                sheets_api.spreadsheets()
                .get(
                    spreadsheetId=spreadsheet_id,
                    includeGridData=True,
                    fields="sheets.properties.title,sheets.properties.sheetType",
                )
                .execute()
            )

        return [
            sheet['title'] for sheet in spreadsheet_data['sheets'] if sheet['sheetType'] == "GRID"
        ]

    def _retrieve_data(self, data_source: GoogleSheetsDataSource) -> pd.DataFrame:
        with self.build_sheets_api() as sheets_api:
            spreadsheet_data = (
                sheets_api.spreadsheets()
                .get(
                    spreadsheetId=data_source.spreadsheet_id,
                    includeGridData=True,
                    fields="sheets.properties.title,sheets.properties.sheetType,sheets.data.rowData.values.effectiveValue,sheets.data.rowData.values.effectiveFormat.numberFormat",
                )
                .execute()
            )

        sheets = [s for s in spreadsheet_data['sheets'] if s['properties']['sheetType'] == 'GRID']
        if data_source.sheet is None:
            # Select the first sheet
            sheet = sheets[0]
        else:
            try:
                sheet = [s for s in sheets if s['properties']['title'] == data_source.sheet][0]
            except KeyError:
                raise InvalidSheetException(
                    f'No sheet named {data_source.sheet} (available sheets: {[s["properties"]["title"] for s in sheets]}'
                )

        values = [
            [get_cell_effective_value(cell) for cell in row['values']]
            for row in sheet['data'][0]['rowData']
        ]

        df = pd.DataFrame(values)

        # Since `data` is a list of lists, the columns are not set properly
        # df =
        #         0            1           2
        #  0  animateur                  week
        #  1    pika                      W1
        #  2    bulbi                     W2
        #
        # We set the first row as the header by default and replace empty value by the index
        # to avoid having errors when trying to jsonify it (two columns can't have the same value)
        df.columns = [name or index for index, name in enumerate(df.iloc[data_source.header_row])]
        df = df[data_source.header_row + 1 :]

        return df


SERIAL_REFERENCE_DAY = datetime.fromisoformat('1899-12-30')


def serial_number_to_date(serial_number: float) -> datetime:
    """
    https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
    """
    return NotImplemented


def get_cell_effective_value(cell):
    if 'effectiveValue' not in cell:
        return None

    if 'stringValue' in cell['effectiveValue']:
        return cell['effectiveValue']['stringValue']

    elif 'numberValue' in cell['effectiveValue']:
        if 'effectiveFormat' in cell and 'numberFormat' in cell['effectiveFormat']:
            if cell['effectiveFormat']['numberFormat']['type'] == 'DATE':
                return serial_number_to_date(cell['effectiveValue']['numberValue'])
        else:
            return cell['effectiveValue']['numberValue']


class GoogleSheetException(Exception):
    ...


class InvalidSheetException(GoogleSheetException):
    ...
