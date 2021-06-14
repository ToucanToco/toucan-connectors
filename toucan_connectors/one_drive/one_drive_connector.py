import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from pydantic import Field

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

API_BASE_ROUTE: str = 'https://graph.microsoft.com/v1.0/me'
AUTHORIZATION_URL: str = 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize'
TOKEN_URL: str = 'https://login.microsoftonline.com/common/oauth2/v2.0/token'


class OneDriveDataSource(ToucanDataSource):
    file: str
    sheet: str
    range: Optional[str]


class OneDriveConnector(ToucanConnector):

    data_source_model: OneDriveDataSource

    _auth_flow = 'oauth2'
    _oauth_trigger = 'instance'
    oauth2_version = Field('1', **{'ui.hidden': True})
    auth_flow_id: Optional[str]

    scope: str = Field(
        None,
        Title='Scope',
        description='The scope determines what type of access the app is granted when the user is signed in',
        placeholder='offline_access Files.Read',
    )

    def __init__(self, **kwargs):
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        # we use __dict__ so that pydantic does not complain about the _oauth2_connector field
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=self.scope,
            token_url=TOKEN_URL,
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
            secrets_keeper=kwargs['secrets_keeper'],
        )

    @staticmethod
    def get_connector_secrets_form() -> ConnectorSecretsForm:
        return ConnectorSecretsForm(
            documentation_md=(Path(os.path.dirname(__file__)) / 'doc.md').read_text(),
            secrets_schema=OAuth2ConnectorConfig.schema(),
        )

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def _get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def _format_url(self, data_source):
        url = f'{API_BASE_ROUTE}/drive/root:/{data_source.file}:/workbook/worksheets/{data_source.sheet}/'

        if data_source.range is None:
            url = url + 'usedRange(valuesOnly=true)'
        else:
            url = url + f"range(address='{data_source.range}')"

        return url

    def _run_fetch(self, url):
        access_token = self._get_access_token()
        headers = {'Authorization': f'Bearer {access_token}'}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()

    def _retrieve_data(self, data_source: OneDriveDataSource) -> pd.DataFrame:

        url = self._format_url(data_source)

        response = self._run_fetch(url)

        data = response.get('values')

        if not data:
            logging.getLogger(__name__).info('No data retrieved from response')

            return pd.DataFrame()

        cols = data[0]
        data.pop(0)

        return pd.DataFrame(data, columns=cols)
