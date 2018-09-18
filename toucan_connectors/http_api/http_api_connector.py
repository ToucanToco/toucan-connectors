import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from pydantic import BaseModel, Schema
from typing import List
import requests
from collections import defaultdict
from jq import jq


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


class AuthType(BaseModel):
    basic = "basic"
    digest = "digest"
    oauth1 = "oauth1"


class Auth(BaseModel):
    type: AuthType
    args: List[str]


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

        REQUESTS_PARAMS = ['url', 'method', 'params', 'data', 'json', 'headers']

        jq_filter = query['filter']
        query = {k: v for k, v in list(query.items()) if k in REQUESTS_PARAMS}
        query['url'] = '/'.join([
            self.baseroute.rstrip('/'),
            query['url'].lstrip('/')
        ])
        # query['auth'] = self.get_auth()
        res = requests.request(**query)

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

        query = data_source.dict()
        
        if hasattr(self, 'template'):
            for k, v in list(self.template.items()):
                query[k].update(v)
                if k in config:
                    query[k].update(config[k])
        data = self._query(query)






        return pd.DataFrame(data)
