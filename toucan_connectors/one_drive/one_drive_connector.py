import logging
from typing import Optional

import pandas as pd
import requests
from pydantic import Field, PrivateAttr, SecretStr

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class OneDriveDataSource(ToucanDataSource):
    site_url: Optional[str] = Field(
        None,
        Title='Site URL',
        description='Access a sharePoint site using the site url (company_name.sharepoint.com/sites/site_name)',
        placeholder='Only for SharePoint',
    )
    document_library: Optional[str] = Field(
        None,
        Title='Document Library',
        description='Access a sharePoint library (Documents)',
        placeholder='Only for SharePoint',
    )
    file: str = Field(
        None,
        Title='File',
        placeholder='Enter your path file',
    )
    sheet: Optional[str] = Field(
        None,
        Title='Sheets',
        description='Read one sheet or append multiple sheets',
        placeholder='Enter a sheet or a comma separated list of sheets',
    )
    range: Optional[str]
    table: Optional[str] = Field(
        None,
        Title='Tables',
        description='Read one table or append multiple tables',
        placeholder='Enter a table or a comma separated list of tables',
    )


class OneDriveConnector(ToucanConnector):

    data_source_model: OneDriveDataSource

    _auth_flow = 'oauth2'
    _oauth_trigger = 'connector'
    oauth2_version = Field('1', **{'ui.hidden': True})
    auth_flow_id: Optional[str]

    authorization_url: str = Field(None, **{'ui.hidden': True})
    token_url: str = Field(None, **{'ui.hidden': True})
    redirect_uri: str = Field(None, **{'ui.hidden': True})
    _oauth2_connector: OAuth2Connector = PrivateAttr()

    client_id: str = Field(
        '',
        title='Client ID',
        description='The client id of you Azure Active Directory integration',
        **{'ui.required': True},
    )
    client_secret: SecretStr = Field(
        '',
        title='Client Secret',
        description='The client secret of your Azure Active Directory integration',
        **{'ui.required': True},
    )
    scope: str = Field(
        None,
        Title='Scope',
        description='The scope determines what type of access the app is granted when the user is signed in',
        placeholder='offline_access Files.Read Sites.Read.All',
    )
    tenant: str = Field(
        None,
        Title='Scope',
        description='The tenant determines what part of your organisation you want to signed in',
        placeholder='common',
    )

    def __init__(self, **kwargs):
        logging.getLogger(__name__).debug(f'Connection params: {kwargs}')
        super().__init__(**{k: v for k, v in kwargs.items() if k != 'secrets_keeper'})

        logging.getLogger(__name__).debug(f'Init: {self.client_id} - {self.client_secret}')

        self.authorization_url = (
            f'https://login.microsoftonline.com/{self.tenant}/oauth2/v2.0/authorize'
        )
        self.token_url = f'https://login.microsoftonline.com/{self.tenant}/oauth2/v2.0/token'

        # we use __dict__ so that pydantic does not complain about the _oauth2_connector field
        self._oauth2_connector = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=self.authorization_url,
            scope=self.scope,
            token_url=self.token_url,
            redirect_uri=self.redirect_uri,
            config=OAuth2ConnectorConfig(
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
            secrets_keeper=kwargs['secrets_keeper'],
        )

    def build_authorization_url(self, **kwargs):
        logging.getLogger(__name__).debug('build_authorization_url')
        return self._oauth2_connector.build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        logging.getLogger(__name__).debug('retrieve_tokens')
        return self._oauth2_connector.retrieve_tokens(authorization_response)

    def _get_access_token(self):
        logging.getLogger(__name__).debug('_get_access_token')
        return self._oauth2_connector.get_access_token()

    def _get_site_id(self, data_source):
        logging.getLogger(__name__).debug('_get_site_id')

        access_token = self._get_access_token()
        headers = {'Authorization': f'Bearer {access_token}'}

        site_url = data_source.site_url.replace('https://', '').rstrip('/')
        baseroute = site_url.split('/', 1)[0]
        endpoint = site_url.split('/', 1)[1]

        url = f'https://graph.microsoft.com/v1.0/sites/{baseroute}:/{endpoint}'
        response = requests.get(url, headers=headers)

        return response.json()['id']

    def _get_list_id(self, data_source, site_id):
        logging.getLogger(__name__).debug('_get_list_id')

        access_token = self._get_access_token()
        headers = {'Authorization': f'Bearer {access_token}'}

        url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists'
        response = requests.get(url, headers=headers)

        for library in response.json()['value']:
            if library['displayName'] == data_source.document_library:
                return library['id']

    def _format_url(self, data_source, workbook_element):
        logging.getLogger(__name__).debug('_format_url')

        # Baseroute for Share Point
        if data_source.site_url and data_source.document_library:
            site_id = self._get_site_id(data_source)
            list_id = self._get_list_id(data_source, site_id)

            url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/drive/root:/{data_source.file}:/workbook/'
        # Baseroute for One Drive
        else:
            url = f'https://graph.microsoft.com/v1.0/me/drive/root:/{data_source.file}:/workbook/'

        # Endpoint for Sheet
        if data_source.sheet:
            url = url + f'worksheets/{workbook_element}/'

            # Param for sheet's range
            if data_source.range:
                url = url + f"range(address='{data_source.range}')"
            # Param for complete sheet
            else:
                url = url + 'usedRange(valuesOnly=true)'
        # Endpoint for Table
        else:
            url = url + f'tables/{workbook_element}/range'

        return url

    def _run_fetch(self, url):
        logging.getLogger(__name__).debug('_run_fetch')
        access_token = self._get_access_token()
        headers = {'Authorization': f'Bearer {access_token}'}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()

    def _retrieve_data(self, data_source: OneDriveDataSource) -> pd.DataFrame:
        logging.getLogger(__name__).debug('_retrieve_data')

        if data_source.sheet and data_source.table:
            raise ValueError('You cannot specifiy both sheets and tables')

        if data_source.range and data_source.table:
            raise ValueError('You cannot specify a range for tables (tables is a kind of range)')

        if not data_source.sheet and not data_source.table:
            raise ValueError('You must specify at least a sheet or a table')

        df_all = pd.DataFrame()

        workbook_elements_list = []
        workbook_key_column = ''
        if data_source.sheet:
            workbook_elements_list = data_source.sheet
            workbook_key_column = '__sheetname__'
        else:
            workbook_elements_list = data_source.table
            workbook_key_column = '__tablename__'
        workbook_elements_list = workbook_elements_list.replace(', ', ',').split(',')

        for workbook_element in workbook_elements_list:
            url = self._format_url(data_source, workbook_element)

            response = self._run_fetch(url)

            data = response.get('values')

            if not data:
                logging.getLogger(__name__).debug('No data retrieved from response')
                return pd.DataFrame()

            cols = data[0]
            data.pop(0)

            df_current = pd.DataFrame(data, columns=cols)
            if len(workbook_elements_list) > 1:
                df_current[workbook_key_column] = workbook_element

            df_all = df_all.append(df_current)

        return df_all

    def get_status(self) -> ConnectorStatus:
        """
        Test the One Drive's connexion.
        :return: a ConnectorStatus with the current status
        """
        try:
            access_token = self._get_access_token()
            if access_token:
                c = ConnectorStatus(status=True)
                return c
            else:
                return ConnectorStatus(status=False)
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')
