"""Google Sheets connector with oauth-manager setup."""

# This will replace the old Google Sheets connector that works with the Bearer API
import asyncio
from contextlib import suppress
from typing import Any, Dict, Optional

import pandas as pd
from aiohttp import ClientSession
from pydantic import Field, create_model

from toucan_connectors.common import ConnectorStatus, HttpError, fetch, get_loop
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum


class GoogleSheets2DataSource(ToucanDataSource):
    """
    Google Spreadsheet 2 data source class.

    Contains:
    - spreadsheet_id
    - sheet
    - header_row
    """

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
    def get_form(cls, connector: 'GoogleSheets2Connector', current_config):
        """Retrieve a form filled with suggestions of available sheets."""
        # Always add the suggestions for the available sheets
        constraints = {}
        with suppress(Exception):
            partial_endpoint = current_config['spreadsheet_id']
            final_url = f'{connector.baseroute}{partial_endpoint}'
            data = connector._run_fetch(final_url, connector.secrets['access_token'])
            available_sheets = [str(x['properties']['title']) for x in data['sheets']]
            constraints['sheet'] = strlist_to_enum('sheet', available_sheets)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


Secrets = Dict[str, Any]


class GoogleSheets2Connector(ToucanConnector):
    """The Google Sheets connector."""

    data_source_model: GoogleSheets2DataSource

    auth_flow = 'oauth2'

    # The following should be hidden properties

    baseroute = 'https://sheets.googleapis.com/v4/spreadsheets/'

    secrets: Optional[Secrets]

    async def _authentified_fetch(self, url, access_token):
        """Build the final request along with headers."""
        headers = {'Authorization': f'Bearer {access_token}'}

        async with ClientSession(headers=headers) as session:
            return await fetch(url, session)

    def set_secrets(self, secrets: Secrets):
        """Set the secrets from inside the main service."""
        self.secrets = secrets

    def _run_fetch(self, url, access_token):
        """Run loop."""
        loop = get_loop()
        future = asyncio.ensure_future(self._authentified_fetch(url, access_token))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: GoogleSheets2DataSource) -> pd.DataFrame:
        """
        Point of entry for data retrieval in the connector

        Requires:
        - Datasource
        """
        if not self.secrets:
            raise Exception('No credentials')

        access_token = self.secrets['access_token']

        if data_source.sheet is None:
            # Get spreadsheet informations and retrieve all the available sheets
            # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
            data = self._run_fetch(f'{self.baseroute}{data_source.spreadsheet_id}', access_token)
            available_sheets = [str(x['properties']['title']) for x in data['sheets']]
            data_source.sheet = available_sheets[0]

        # https://developers.google.com/sheets/api/samples/reading
        read_sheet_endpoint = f'{data_source.spreadsheet_id}/values/{data_source.sheet}'
        full_url = f'{self.baseroute}{read_sheet_endpoint}'

        data = self._run_fetch(full_url, access_token)['values']
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

    def get_status(self) -> ConnectorStatus:
        """
        Test the Google Sheets connexion.

        If successful, returns a message with the email of the connected user account.
        """
        if not self.secrets or 'access_token' not in self.secrets:
            return ConnectorStatus(status=False, error='Credentials are missing')

        access_token = self.secrets['access_token']
        try:
            user_info = self._run_fetch(
                'https://www.googleapis.com/oauth2/v2/userinfo?alt=json', access_token
            )
            return ConnectorStatus(status=True, message=f"Connected as {user_info.get('email')}")
        except HttpError:
            return ConnectorStatus(status=False, error="Couldn't retrieve user infos")
