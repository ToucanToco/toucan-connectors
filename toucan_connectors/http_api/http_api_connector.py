from enum import Enum
from typing import List, Union

import pandas as pd
from jq import jq
from pydantic import BaseModel, FilePath, Schema
from requests import Session

from toucan_connectors.auth import Auth
from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


def transform_with_jq(data: object, jq_filter: str) -> list:
    data = jq(jq_filter).transform(data, multiple_output=True)

    # jq "multiple outout": the data is already presented as a list of rows
    multiple_output = len(data) == 1 and isinstance(data[0], list)

    # another valid datastructure:  [{col1:[value, ...], col2:[value, ...]}]
    single_cols_dict = isinstance(data[0], dict) and isinstance(list(data[0].values())[0], list)

    if multiple_output or single_cols_dict:
        return data[0]

    return data


class Method(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"


class Template(BaseModel):
    headers: dict = None
    params: dict = None
    json_: dict = Schema(None, alias='json')
    proxies: dict = None


class HttpAPIDataSource(ToucanDataSource):
    url: str
    method: Method = Method.GET
    headers: dict = None
    params: dict = None
    json_: dict = Schema(None, alias='json')
    proxies: dict = None
    data: Union[str, dict] = None
    filter: str = "."


class HttpAPIConnector(ToucanConnector):
    data_source_model: HttpAPIDataSource

    baseroute: str
    cert: List[FilePath] = None
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

        available_params = ['url', 'method', 'params', 'data', 'json', 'headers', 'proxies']
        query = {k: v for k, v in query.items() if k in available_params}
        query['url'] = '/'.join([self.baseroute.rstrip('/'), query['url'].lstrip('/')])
        if self.cert:
            # `cert` is a list of PosixPath. `request` needs a list of strings for certificates
            query['cert'] = [str(c) for c in self.cert]
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

    def _retrieve_data(self, data_source: HttpAPIDataSource) -> pd.DataFrame:

        if self.auth:
            session = self.auth.get_session()
        else:
            session = Session()

        query = nosql_apply_parameters_to_query(
            data_source.dict(by_alias=True), data_source.parameters
        )

        if self.template:
            template = {k: v for k, v in self.template.dict(by_alias=True).items() if v}
            for k in query.keys() & template.keys():
                if query[k]:
                    template[k].update(query[k])
                query[k] = template[k]

        return pd.DataFrame(self.do_request(query, session))
