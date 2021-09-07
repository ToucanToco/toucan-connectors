"""Google Sheets connector with oauth-manager setup."""

# This will replace the old Google Sheets connector that works with the Bearer API
import asyncio
import os
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import pandas as pd
from aiohttp import ClientSession
from pydantic import Field, PrivateAttr, create_model

from toucan_connectors.common import ConnectorStatus, HttpError, fetch, get_loop
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    DataSlice,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

AUTHORIZATION_URL: str = (
    'https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent'
)
SCOPE: str = 'openid email https://www.googleapis.com/auth/spreadsheets.readonly'
TOKEN_URL: str = 'https://oauth2.googleapis.com/token'


class NoCredentialsError(Exception):
    """Raised when no secrets avaiable."""


class GoogleSheets2DataSource(ToucanDataSource):
    """
    Google Spreadsheet 2 data source class.

    Contains:
    - spreadsheet_id
    - sheet
    - header_row
    """

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
    rows_limit: int = Field(
        None, title='Rows limit', description='Maximum number of rows to retrieve'
    )
    parameters: dict = Field(None, description='Additional URL parameters')
    parse_dates: List[str] = Field([], title='Dates column', description='Columns to parse as date')

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['GoogleSheets2DataSource']) -> None:
            keys = schema['properties'].keys()
            prio_keys = ['domain', 'spreadsheet_id', 'sheet']
            new_keys = prio_keys + [k for k in keys if k not in prio_keys]
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}

    @classmethod
    def get_form(cls, connector: 'GoogleSheets2Connector', current_config, **kwargs):
        """Retrieve a form filled with suggestions of available sheets."""
        # Always add the suggestions for the available sheets
        constraints = {}
        with suppress(Exception):
            partial_endpoint = current_config['spreadsheet_id']
            final_url = f'{connector._baseroute}{partial_endpoint}'
            data = connector._run_fetch(final_url)
            available_sheets = [str(x['properties']['title']) for x in data['sheets']]
            constraints['sheet'] = strlist_to_enum('sheet', available_sheets)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class GoogleSheets2Connector(ToucanConnector):
    """The Google Sheets connector."""

    data_source_model: GoogleSheets2DataSource

    _auth_flow = 'oauth2'
    _oauth_trigger = 'instance'
    oauth2_version = Field('1', **{'ui.hidden': True})
    auth_flow_id: Optional[str]

    # TODO: turn into a class property
    _baseroute = 'https://sheets.googleapis.com/v4/spreadsheets/'

    _oauth2_connector: OAuth2Connector = PrivateAttr()

    @staticmethod
    def get_connector_secrets_form() -> ConnectorSecretsForm:
        return ConnectorSecretsForm(
            documentation_md=(Path(os.path.dirname(__file__)) / 'doc.md').read_text(),
            secrets_schema=OAuth2ConnectorConfig.schema(),
        )

    def __init__(self, **kwargs):
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        # we use __dict__ so that pydantic does not complain about the _oauth2_connector field
        self._oauth2_connector = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=SCOPE,
            token_url=TOKEN_URL,
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
            secrets_keeper=kwargs['secrets_keeper'],
        )

    def build_authorization_url(self, **kwargs):
        return self._oauth2_connector.build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self._oauth2_connector.retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self._oauth2_connector.get_access_token()

    async def _fetch(self, url, headers=None):
        """Build the final request along with headers."""
        async with ClientSession(headers=headers) as session:
            return await fetch(url, session)

    def _run_fetch(self, url):
        """Run loop."""
        access_token = self.get_access_token()
        if not access_token:
            raise NoCredentialsError('No credentials')
        headers = {'Authorization': f'Bearer {access_token}'}

        loop = get_loop()
        future = asyncio.ensure_future(self._fetch(url, headers))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: GoogleSheets2DataSource) -> pd.DataFrame:
        """
        Point of entry for data retrieval in the connector

        Requires:
        - Datasource
        - Secrets
        """
        if data_source.sheet is None:
            # Get spreadsheet informations and retrieve all the available sheets
            # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
            data = self._run_fetch(f'{self._baseroute}{data_source.spreadsheet_id}')
            available_sheets = [str(x['properties']['title']) for x in data['sheets']]
            data_source.sheet = available_sheets[0]

        if data_source.rows_limit:
            ranges = f'!1:{data_source.rows_limit+1}'
        else:
            ranges = ''

        # https://developers.google.com/sheets/api/samples/reading
        read_sheet_endpoint = f'{data_source.spreadsheet_id}/values/{data_source.sheet}{ranges}?valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING'
        full_url = f'{self._baseroute}{read_sheet_endpoint}'
        # Rajouter le param FORMATTED_VALUE pour le séparateur de décimal dans la Baseroute
        data = self._run_fetch(full_url)['values']
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

        for date_column in data_source.parse_dates:
            df[date_column] = pd.to_datetime(df[date_column])
        return df

    def get_status(self) -> ConnectorStatus:
        """
        Test the Google Sheets connexion.

        If successful, returns a message with the email of the connected user account.
        """
        try:
            access_token = self.get_access_token()
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')

        if not access_token:
            return ConnectorStatus(status=False, error='Credentials are missing')

        try:
            user_info = self._run_fetch('https://www.googleapis.com/oauth2/v2/userinfo?alt=json')
            return ConnectorStatus(status=True, message=f"Connected as {user_info.get('email')}")
        except HttpError:
            return ConnectorStatus(status=False, error="Couldn't retrieve user infos")

    def get_slice(
        self,
        data_source: GoogleSheets2DataSource,
        permissions: Optional[dict] = None,
        offset: int = 0,
        limit=50,
        get_row_count: Optional[bool] = False,
    ) -> DataSlice:
        """
        Method to retrieve a part of the data as a pandas dataframe
        and the total size filtered with permissions

        - offset is the index of the starting row
        - limit is the number of pages to retrieve
        Exemple: if offset = 5 and limit = 10 then 10 results are expected from 6th row
        """
        preview_datasource = GoogleSheets2DataSource(
            domain=data_source.domain,
            name=data_source.name,
            spreadsheet_id=data_source.spreadsheet_id,
            sheet=data_source.sheet,
            header_row=data_source.header_row,
            rows_limit=limit,
            parse_dates=data_source.parse_dates,
        )
        df = self.get_df(preview_datasource, permissions)
        if limit is not None:
            return DataSlice(df[offset : offset + limit], len(df))
        else:
            return DataSlice(df[offset:], len(df))
