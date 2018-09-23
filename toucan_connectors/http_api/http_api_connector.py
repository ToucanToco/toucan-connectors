from enum import Enum

import pandas as pd
from pydantic import BaseModel
from typing import List
from jq import jq

from requests import request
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from requests_oauthlib import OAuth1

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


class AuthType(Enum):
    basic = "basic"
    digest = "digest"
    oauth1 = "oauth1"


class Auth(BaseModel):
    type: AuthType
    args: List[str]

    def get_auth(self):
        auth_class = {
            'basic': HTTPBasicAuth,
            'digest': HTTPDigestAuth,
            'oauth1': OAuth1,
        }.get(self.type.value)

        return auth_class(*self.args)


class Method(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"


class HttpAPIDataSource(ToucanDataSource):
    url: str
    method: Method = "GET"
    headers: dict = None
    params: dict = None
    _json: dict = None
    data: str = None
    filter: str = "."
    auth: Auth = None
    parameters: dict = None

    class Config:
        fields = {'_json': {'alias': 'json'}}


class HttpAPIConnector(ToucanConnector):
    type = "HttpAPI"
    data_source_model: HttpAPIDataSource

    baseroute: str
    auth: Auth = None

    def do_request(self, query, auth):
        """
        Get some json data with an HTTP request and run a jq filter on it.
        Args:
            query (dict): specific infos about the request (url, http headers, etc.)
            auth: one of request Auth objects
        Returns:
            data (list): The response from the API in the form of a list of dict
        """

        available_params = ['url', 'method', 'params', 'data', 'json', 'headers', "auth"]

        jq_filter = query['filter']
        query = {k: v for k, v in query.items() if k in available_params}
        query['url'] = '/'.join([self.baseroute.rstrip('/'), query['url'].lstrip('/')])
        query['auth'] = auth

        res = request(**query)

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

        # generate the request auth object
        if data_source.auth:
            auth = data_source.auth.get_auth()
            data_source.auth = None
        else:
            auth = None

        query = nosql_apply_parameters_to_query(
            data_source.dict(),
            data_source.parameters)

        return pd.DataFrame(self.do_request(query, auth))
