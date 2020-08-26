"""Google Sheets connector with oauth-manager setup."""

# This will replace the old Google Sheets connector that works with the Bearer API
from typing import Optional

import pandas as pd
from pydantic import Field

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class GoogleSheets2DataSource(ToucanDataSource):
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


class GoogleSheets2Connector(ToucanConnector):
    data_source_model: GoogleSheets2DataSource

    auth_flow = 'oauth2'
    access_token: str

    def _retrieve_data(self, data_source: GoogleSheets2DataSource) -> pd.DataFrame:
        pass
