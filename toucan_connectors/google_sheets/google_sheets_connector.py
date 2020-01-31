from contextlib import suppress
from typing import Optional

import pandas as pd
from pydantic import Field, create_model

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum


class GoogleSheetsDataSource(ToucanDataSource):
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

    @classmethod
    def get_form(cls, connector: 'GoogleSheetsConnector', current_config):
        # Always add the suggestions for the available sheets
        constraints = {}
        with suppress(Exception):
            data = connector.bearer_oauth_get_endpoint(current_config['spreadsheet_id'])
            available_sheets = [str(x['properties']['title']) for x in data['sheets']]
            constraints['sheet'] = strlist_to_enum('sheet', available_sheets)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class GoogleSheetsConnector(ToucanConnector):
    """
    This is a connector for [GoogleSheets](https://developers.google.com/sheets/api/reference/rest)
    using [Bearer.sh](https://app.bearer.sh/)
    """

    data_source_model: GoogleSheetsDataSource
    bearer_integration = 'google_sheets'
    bearer_auth_id: str

    def _retrieve_data(self, data_source: GoogleSheetsDataSource) -> pd.DataFrame:
        if data_source.sheet is None:
            # Get spreadsheet informations and retrieve all the available sheets
            # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
            data = self.bearer_oauth_get_endpoint(data_source.spreadsheet_id)
            available_sheets = [str(x['properties']['title']) for x in data['sheets']]
            data_source.sheet = available_sheets[0]

        # https://developers.google.com/sheets/api/samples/reading
        read_sheet_endpoint = f'{data_source.spreadsheet_id}/values/{data_source.sheet}'
        data = self.bearer_oauth_get_endpoint(read_sheet_endpoint)['values']
        df = pd.DataFrame(data)

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
