import pandas as pd
import requests
from pydantic import Field

from toucan_connectors.common import FilterSchema, transform_with_jq
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class RokDataSource(ToucanDataSource):
    database: str
    query: str = Field(..., description='GQL string')
    filter: str = FilterSchema


class RokConnector(ToucanConnector):
    data_source_model: RokDataSource

    host: str
    username: str
    password: str

    def _retrieve_data(self, data_source: RokDataSource) -> pd.DataFrame:

        endpoint = f'{self.host}/graphql?DatabaseName={data_source.database}'

        auth_query = """
            query Auth($database: String!, $user: String!, $password: String!)
            {authenticate(database: $database, user: $user, password: $password)}"""
        auth_vars = {
            'database': data_source.database,
            'user': self.username,
            'password': self.password,
        }
        auth_res = requests.post(
            endpoint, json={'query': auth_query, 'variables': auth_vars}
        ).json()
        if 'errors' in auth_res:
            raise ValueError(str(auth_res['errors']))

        payload = {'query': data_source.query, 'variables': data_source.parameters}
        headers = {'Token': auth_res['data']['authenticate']}
        res = requests.post(endpoint, json=payload, headers=headers).json()
        if 'errors' in res:
            raise ValueError(str(res['errors']))

        return pd.DataFrame(transform_with_jq(res, data_source.filter))
