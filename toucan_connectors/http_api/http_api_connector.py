from collections import defaultdict
from enum import Enum

import pandas as pd
from pydantic import BaseModel, Schema
from typing import List
from jq import jq

from requests import request
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from requests_oauthlib import OAuth1

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import nosql_apply_parameters_to_query


def transform_with_jq(data: object, jq_filter: str) -> list:
    """
    Our standard way to apply a jq filter on data before it's passed to a pd.DataFrame
    """
    data = jq(jq_filter).transform(data, multiple_output=True)

    # If the data is already presented as a list of rows,
    # then undo the nesting caused by "multiple_output" jq option
    if len(data) == 1 and (isinstance(data[0], list)
        # detects another valid datastructure [{col1:[value, ...], col2:[value, ...]}]
        or (isinstance(data[0], dict) and isinstance(list(data[0].values())[0],list))):
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


class Method(BaseModel):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"


class HttpAPIDataSource(ToucanDataSource):
    url: str
    method: Method = "GET"
    headers: dict = None
    params: dict = None
    _json: dict = Schema(None, alias = 'json')
    data: str = None
    filter: str = "."
    auth: Auth = None
    parameters: dict = None


class HttpAPIConnector(ToucanConnector):
    type = "HttpAPI"
    data_source_model: HttpAPIDataSource
    
    baseroute: str
    auth : Auth = None

    def _query(self, query):
        """
        Get some json data with an HTTP request and run a jq filter on it.
        Args:
            query (dict): specific infos about the request (url, http headers, etc.)
        Returns:
            data (list): The response from the API in the form of a list of dict
        """

        REQUESTS_PARAMS = ['url', 'method', 'params', 'data', 'json', 'headers', "auth"]

        jq_filter = query['filter']
        query = {k: v for k, v in list(query.items()) if k in REQUESTS_PARAMS}
        query['url'] = '/'.join([
            self.baseroute.rstrip('/'),
            query['url'].lstrip('/')
        ])

        res = request(**query)

        try:
            data = res.json()
        except ValueError:
            logger.error(f'Could not decode "{res.content}"')
            raise

        try:
            return transform_with_jq(data, jq_filter)
        except ValueError:
            logger.error(f'Could not transform {data} using {jq_filter}')
            raise


    def get_df(self, data_source: HttpAPIDataSource) -> pd.DataFrame:

        # generate the request auth object
        if data_source.auth :
            auth = data_source.auth.get_auth()
        else :
            auth = None
        data_source.auth = None
        query = nosql_apply_parameters_to_query(data_source.dict(), data_source.parameters)

        # inject the request auth object
        query['auth'] = auth
        
        data = self._query(query)

        return pd.DataFrame(data)
