import logging
import re
from typing import List, Optional

import pandas as pd
import requests
from pydantic import Field, PrivateAttr, SecretStr

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class NotFoundError(Exception):
    """ """


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
    match: bool = Field(
        False, Title='Match', description='Try to match files using provided file name'
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
    parse_dates: Optional[List[str]] = Field(
        [],
        Title='Date columns',
        description='By default, dates are converted in the number of days since 1900/01/01',
        placeholder='Enter your date columns',
    )


def _prepare_workbook_elements(data_source):
    if data_source.sheet:
        workbook_elements_list = data_source.sheet
        workbook_key_column = '__sheetname__'
    else:
        workbook_elements_list = data_source.table
        workbook_key_column = '__tablename__'
    workbook_elements_list = [a.strip() for a in workbook_elements_list.split(',')]
    return workbook_elements_list, workbook_key_column


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

    def _format_url(self, data_source, workbook_element, file):
        logging.getLogger(__name__).debug('_format_url')

        url = self._build_url_root(data_source)
        url += f'/{file}:/workbook/'

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

    def _format_urls(self, data_source, workbook_element, filenames):
        logging.getLogger(__name__).debug('_format_url')
        return [self._format_url(data_source, workbook_element, file) for file in filenames]

    def _build_url_root(self, data_source):
        # Baseroute for Share Point
        if data_source.site_url and data_source.document_library:
            site_id = self._get_site_id(data_source)
            list_id = self._get_list_id(data_source, site_id)

            url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/drive/root:'
        # Baseroute for One Drive
        else:
            url = 'https://graph.microsoft.com/v1.0/me/drive/root:'
        return url

    def _run_fetch(self, url):
        logging.getLogger(__name__).debug('_run_fetch')
        access_token = self._get_access_token()
        headers = {'Authorization': f'Bearer {access_token}'}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()

    def _retrieve_files_path(self, data_source: OneDriveDataSource) -> List[str]:
        logging.getLogger(__name__).debug('_retrieve_files_path')
        path = None
        # Split the "file" input to retrieve the path & the pattern
        splitted = data_source.file.split('/')
        if len(splitted) > 1:
            path = f"/{('/').join(splitted[:len(splitted)-1])}:"
            pattern = splitted[-1]
        else:
            pattern = splitted[0]

        url = self._build_url_root(data_source)
        if path:
            url += f'{path}/children'
        else:
            url = f'{url[:-1]}/children'
        response = requests.get(
            url,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self._get_access_token()}',
            },
        )
        response.raise_for_status()
        children = response.json().get('value')
        if children:
            val = list(
                filter(
                    lambda f: re.match(re.compile(pattern), f),
                    [c['name'] for c in children if 'folder' not in c],
                )
            )
            if path:
                return [f'{path[1:-1] if path else ""}/{v}' for v in val]
            else:
                return [f'/{v}' for v in val]
        else:
            raise NotFoundError('No matching file name')

    def _retrieve_data(self, data_source: OneDriveDataSource) -> pd.DataFrame:
        logging.getLogger(__name__).debug('_retrieve_data')

        if data_source.sheet and data_source.table:
            raise ValueError('You cannot specifiy both sheets and tables')

        if data_source.range and data_source.table:
            raise ValueError('You cannot specify a range for tables (tables is a kind of range)')

        if not data_source.sheet and not data_source.table:
            raise ValueError('You must specify at least a sheet or a table')

        file_names = None
        if data_source.match:
            file_names = self._retrieve_files_path(data_source)

        df_all = pd.DataFrame()
        workbook_elements_list, workbook_key_column = _prepare_workbook_elements(data_source)

        for workbook_element in workbook_elements_list:
            if file_names:
                urls = self._format_urls(data_source, workbook_element, file_names)
            else:
                urls = [self._format_url(data_source, workbook_element, data_source.file)]

            def url_yielder():
                for url in urls:
                    try:
                        yield self._run_fetch(url).get('values')
                    except requests.exceptions.HTTPError:
                        logging.getLogger(__name__).warning(
                            f'Fetch failed for {url}, maybe sheet, range or table are invalid for this file'
                        )
                        # TODO: some day make it async

            data = list(url_yielder())

            for d in [d for d in data if d]:
                cols = d[0]
                d.pop(0)
                df_current = pd.DataFrame(d, columns=cols)

                if len(workbook_elements_list) > 1:
                    df_current[workbook_key_column] = workbook_element
                df_all = df_all.append(df_current)

        if data_source.parse_dates:
            for date_col in data_source.parse_dates:
                try:
                    df_all[date_col] = pd.to_datetime(
                        (df_all[date_col] * 24 * 60 * 60).astype(int),
                        origin=pd.Timestamp('1899-12-30'),
                        unit='s',
                    )
                except ValueError:
                    raise ValueError(f"Cannot convert column '{date_col}' to datetime")

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
