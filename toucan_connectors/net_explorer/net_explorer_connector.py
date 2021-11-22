import json
import logging
from io import BytesIO
from typing import Optional

import pandas as pd
import requests
from pydantic import Field, SecretStr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class NetExplorerDataSource(ToucanDataSource):
    file: str
    sheet: Optional[str] = 0


class NetExplorerConnector(ToucanConnector):
    data_source_model: NetExplorerDataSource
    instance_url: str = Field(
        None,
        Title='Instance URL',
        placeholder='exemple.netexplorer.pro',
    )
    user: str
    password: SecretStr

    def _retrieve_token(self):
        login_url = f'https://{self.instance_url}/api/auth'

        data = json.dumps({'user': self.user, 'password': self.password.get_secret_value()})

        headers = {'Content-Type': 'application/json'}

        resp = requests.request('POST', login_url, data=data, headers=headers)

        return resp.json()['token']

    def _retrieve_folders(self, token):
        folders_url = f'https://{self.instance_url}/api/folders?depth=-1'

        headers = {'Authorization': f'Bearer {token}'}

        resp = requests.request('GET', folders_url, data={}, headers=headers)

        return resp.json()

    def _retrieve_file_id(self, folders, data_source):

        basedir = data_source.file.split('/')[0]
        path = data_source.file.split('/')[1:]

        id = None

        try:
            # Search among base directories
            for folder in folders:
                if folder['name'] == basedir:
                    folders = folder['content']
                    break

            # Serch among paths
            for elem in path:
                if elem.endswith(('xlsx', 'xls', 'csv')):
                    for file in folders['files']:
                        if file['name'] == elem:
                            id = file['id']
                            break
                else:
                    for folder in folders['folders']:
                        if folder['name'] == elem:
                            folders = folder['content']

            assert id
        except AssertionError:
            raise ValueError('Unable to find the file')

        return id

    def _retrieve_file(self, token, id):
        download_url = f'https://{self.instance_url}/api/file/{id}/download'

        headers = {'Authorization': f'Bearer {token}'}

        resp = requests.request('GET', download_url, data={}, headers=headers)

        return BytesIO(resp.content)

    def _retrieve_data(self, data_source: NetExplorerDataSource) -> pd.DataFrame:
        logging.getLogger(__name__).debug('_retrieve_data')

        self.instance_url = self.instance_url.replace('https://', '').strip('/')
        data_source.file = data_source.file.strip('/')

        token = self._retrieve_token()
        folders = self._retrieve_folders(token)
        id = self._retrieve_file_id(folders, data_source)
        data = self._retrieve_file(token, id)

        df = pd.DataFrame()
        if data_source.file.endswith('csv'):
            df = pd.read_csv(data)
        else:
            df = pd.read_excel(data, sheet_name=data_source.sheet)

        return df
