import base64
from datetime import datetime, timedelta

import pandas as pd
import requests
from jwt import encode
from pydantic import Field
from simplejson import JSONDecodeError

from toucan_connectors.common import (
    FilterSchema,
    nosql_apply_parameters_to_query,
    transform_with_jq,
)
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class RokDataSource(ToucanDataSource):
    database: str
    query: str = Field(..., description='GQL string')
    filter: str = FilterSchema
    start_date: str = None
    end_date: str = None
    viewId: str = None


class InvalidJWTError(Exception):
    """
    Raised when the response from ROK is not JSON deserializable,
    it occurs when our request is invalid because JWT was not validated
    by ROK
    """


class InvalidUsernameError(Exception):
    """
    Raised when the connector tries to be authenticated
    with an unrecognized username in the JWT claims
    """


class RokConnector(ToucanConnector):
    data_source_model: RokDataSource
    host: str
    username: str
    password: str = None
    secret: str = None
    authenticated_with_token: bool = False

    def _retrieve_data(self, data_source: RokDataSource) -> pd.DataFrame:
        # Endpoint depends on the authentication mode
        endpoint = f'{self.host}/graphql'
        date_viewid_parameters = {
            'start_date': data_source.start_date,
            'end_date': data_source.end_date,
            'viewId': data_source.viewId,
        }

        if data_source.parameters:
            parameters = {**data_source.parameters, **date_viewid_parameters}
        else:
            parameters = date_viewid_parameters
        data_source.query = nosql_apply_parameters_to_query(data_source.query, parameters)

        if self.authenticated_with_token:
            if not data_source.live_data:
                raise InvalidAuthenticationMethodError(
                    """Request with ROK token is not possible while not
                     in live data mode. Change the connector configuration to live data"""
                )
            if not self.secret:
                raise NoROKSecretAvailableError('secrets not defined')
            res = self.retrieve_data_with_jwt(data_source, endpoint)

        else:
            endpoint = f'{endpoint}?DatabaseName={data_source.database}'
            # First retrieve the authentication token
            rok_token = self.retrieve_token_with_password(data_source.database, endpoint)
            # Then retrieve the data
            payload = {'query': data_source.query}
            res = requests.post(endpoint, json=payload, headers={'Token': rok_token}).json()

        if 'errors' in res:
            raise ValueError(str(res['errors']))

        return pd.DataFrame(transform_with_jq(res, data_source.filter))

    def retrieve_data_with_jwt(self, data_source: RokDataSource, endpoint: str) -> str:
        """Query ROK API with JWT crafted based on the ROK secret to get the Data"""
        # Claims defined with ROK
        payload = {
            'aud': 'Rok-solution',
            'iss': 'ToucanToco',
            'exp': str(int((datetime.now() + timedelta(minutes=10)).timestamp())),
            'email': self.username,
            'iat': str(int(datetime.now().timestamp())),
            'nbf': str(int(datetime.now().timestamp())),
        }

        encoded_payload = encode(
            payload, base64.b64decode(self.secret.encode('utf-8')), algorithm='HS256'
        )
        headers = {
            'DatabaseName': data_source.database,
            'JwtString': encoded_payload,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        try:
            res = requests.post(
                url=endpoint, data=JsonWrapper.dumps({'query': data_source.query}), headers=headers
            ).json()

        except JSONDecodeError:
            raise InvalidJWTError('Invalid request, JWT not validated by ROK')

        if res.get('Message'):
            if 'not authenticated' in res['Message']:
                raise InvalidUsernameError('Invalid username')
            else:
                raise ValueError(res['Message'])

        return res

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
