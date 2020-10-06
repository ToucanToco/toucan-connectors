from datetime import datetime
import ipdb
import pandas as pd
import requests
from jwt import encode
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
    secret: str  = None


    def _retrieve_data(self, data_source: RokDataSource) -> pd.DataFrame:

        # Endpoint to be clarified with ROK for both mode
        endpoint = f'{self.host}/graphql?DatabaseName={data_source.database}'

        if self.authentified_with_rok_token:
            if not data_source.live_data:
                raise InvalidAuthenticationMethodError(
                    """Request with ROK token is not possible while not
                     in live data mode. Change the connector configuration to live data"""
                )
            if not self.secret:
                raise NoROKSecretAvailableError('secret not defined')

            rok_token = self.retrieve_token_with_jwt(data_source.database, endpoint)        
        else:

            rok_token = self.retrieve_token_with_password(data_source.database, endpoint)

        res = requests.post(endpoint, json=payload, headers=headers).json()

        if 'errors' in res:
            raise ValueError(str(res['errors']))

        return pd.DataFrame(transform_with_jq(res, data_source.filter))


    def retrieve_token_with_jwt(self, database: str, endpoint: str) -> str:
        """Query ROK API with JWT crafted based on the ROK secret to get the ROK token"""
        auth_query = """
        query Auth($database: String!, $token: String!)
        {authenticateUsingJWT(database: $database, token: $token)}"""
        
        payload = {
            'database': database,
            'username': self.username,
            'iat': datetime.utcnow(),
        }

        encoded_payload = encode(payload, self.secret, algorithm='HS256')
        auth_vars = {
                'database': database,
                'jwt_token': encoded_payload.decode('utf-8'),
            }
        auth_res = requests.post(
            url=endpoint, json={'query': auth_query, 'variables': auth_vars}
        ).json()
        if 'errors' in auth_res:
            raise ValueError(str(auth_res['errors']))
    
        return auth_res['data']['token']

    def retrieve_token_with_password(self, database: str, endpoint: str) -> str:
        """Query ROK API with username & password to get the ROK token"""
        auth_query = """
        query Auth($database: String!, $user: String!, $password: String!)
        {authenticate(database: $database, user: $user, password: $password)}"""
        auth_vars = {
            'database': database,
            'user': self.username,
            'password': self.password,
        }
        auth_res = requests.post(
            url=endpoint, json={'query': auth_query, 'variables': auth_vars}
        ).json()

        if 'errors' in auth_res:
            raise ValueError(str(auth_res['errors']))   

        return auth_res['data']['authenticate']

class InvalidAuthenticationMethodError(Exception):
    """Raised when a user tries to use a ROK token while not in live Data Mode"""


class NoROKSecretAvailableError(Exception):
    """Raised if a user try to use the ROK token authentication method without retrievable secret"""
