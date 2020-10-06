import os
from datetime import datetime

import pandas as pd
import requests
from jwt import decode, encode
from pydantic import Field
from datetime import datetime
from toucan_connectors.common import FilterSchema, transform_with_jq
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from jwt import encode, decode
import os


class RokDataSource(ToucanDataSource):
    database: str
    query: str = Field(..., description='GQL string')
    filter: str = FilterSchema


class RokConnector(ToucanConnector):
    data_source_model: RokDataSource
    host: str
    username: str 
    password: str
    secret: str  = None


    def _retrieve_data(self, data_source: RokDataSource) -> pd.DataFrame:

        endpoint = f'{self.host}/graphql'

        if data_source.live_data:
            auth_query = """
            query Auth($database: String!, $token: String!)
            {authenticateUsingJWT(database: $database, token: $token)}"""

            payload = {
            'database':data_source.database,
            'username':self.username,
            'iat': datetime.utcnow()
            }

            if not self.secret:
                raise ValueError('secret not defined')

            encoded_payload = encode(payload, self.secret, algorithm='HS256')
            auth_vars = {
                'database': data_source.database,
                'jwt_token': encoded_payload.decode('utf-8')
            }
            auth_res = requests.post(
                endpoint, json={'query': auth_query, 'variables': auth_vars}
            ).json()
            rok_token = auth_res['data']['token']
            payload = {'query': data_source.query, 'variables': data_source.parameters}
            headers = {'Token': rok_token}

        else:
            endpoint = f'{endpoint}?DatabaseName={data_source.database}'

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
