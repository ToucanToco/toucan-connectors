from enum import Enum

import pandas as pd
from pydantic import BaseModel
from typing import List
from jq import jq

from requests import Session
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from requests_oauthlib import OAuth1, OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import nosql_apply_parameters_to_query


def transform_with_jq(data: object, jq_filter: str) -> list:
    data = jq(jq_filter).transform(data, multiple_output=True)

    # jq "multiple outout": the data is already presented as a list of rows
    multiple_output = len(data) == 1 and isinstance(data[0], list)

    # another valid datastructure:  [{col1:[value, ...], col2:[value, ...]}]
    single_cols_dict = isinstance(data[0], dict) and isinstance(list(data[0].values())[0], list)

    if multiple_output or single_cols_dict:
        return data[0]

    return data


def oauth2_backend(token_url, client_id, client_secret):
    oauthclient = BackendApplicationClient(client_id=client_id)
    oauthsession = OAuth2Session(client=oauthclient)
    token = oauthsession.fetch_token(
        token_url=token_url, client_id=client_id, client_secret=client_secret)
    return OAuth2Session(client_id=client_id, token=token)


class AuthType(str, Enum):
    basic = "basic"
    digest = "digest"
    oauth1 = "oauth1"
    oauth2_backend = "oauth2_backend"


class Auth(BaseModel):
    type: AuthType
    args: List[str]

    def get_session(self) -> Session:
        auth_class = {
            'basic': HTTPBasicAuth,
            'digest': HTTPDigestAuth,
            'oauth1': OAuth1,
            'oauth2_backend': oauth2_backend
        }.get(self.type.value)

        auth_instance = auth_class(*self.args)

        # Some authentification mechanisms are built-in a Session...
        if isinstance(auth_instance, Session):
            return auth_instance

        # ... but other are just added as the auth attr of the Session
        session = Session()
        session.auth = auth_instance
        return session


class Method(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"


class Template(BaseModel):
    headers: dict = None
    params: dict = None
    json: dict = None


class HttpAPIDataSource(ToucanDataSource):
    url: str
    method: Method = Method.GET
    headers: dict = None
    params: dict = None
    json: dict = None
    data: str = None
    filter: str = "."
    parameters: dict = None


class HttpAPIConnector(ToucanConnector):
    type = "HttpAPI"
    data_source_model: HttpAPIDataSource

    baseroute: str
    auth: Auth = None
    template: Template = None

    def do_request(self, query, session):
        """
        Get some json data with an HTTP request and run a jq filter on it.
        Args:
            query (dict): specific infos about the request (url, http headers, etc.)
            session (requests.Session):
        Returns:
            data (list): The response from the API in the form of a list of dict
        """
        jq_filter = query['filter']

        available_params = ['url', 'method', 'params', 'data', 'json', 'headers']
        query = {k: v for k, v in query.items() if k in available_params}
        query['url'] = '/'.join([self.baseroute.rstrip('/'), query['url'].lstrip('/')])

        res = session.request(**query)

        try:
            data = res.json()
        except ValueError:
            HttpAPIConnector.logger.error(f'Could not decode "{res.content}"')
            raise

        try:
            return transform_with_jq(data, jq_filter)
        except ValueError:
            HttpAPIConnector.logger.error(f'Could not transform {data} using {jq_filter}')
            raise

    def get_df(self, data_source: HttpAPIDataSource) -> pd.DataFrame:

        if self.auth:
            session = self.auth.get_session()
        else:
            session = Session()

        query = nosql_apply_parameters_to_query(
            data_source.dict(),
            data_source.parameters)

        if self.template:
            template = {k: v for k, v in self.template.dict().items() if v}
            for k in query.keys() & template.keys():
                if query[k]:
                    template[k].update(query[k])
                query[k] = template[k]

        return pd.DataFrame(self.do_request(query, session))
