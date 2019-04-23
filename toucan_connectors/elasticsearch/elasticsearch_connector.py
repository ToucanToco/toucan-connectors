from enum import Enum
from jq import jq
from typing import List, Union
from urllib.parse import urlparse

import pandas as pd
from elasticsearch import Elasticsearch
from pandas.io.json import json_normalize
from pydantic import BaseModel

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import (
    ToucanConnector, ToucanDataSource
)


class ElasticsearchHost(BaseModel):
    url: str
    port: int = None
    username: str = None
    password: str = None
    headers: dict = None


class SearchMethod(str, Enum):
    search = "search"
    msearch = "msearch"


class ElasticsearchDataSource(ToucanDataSource):
    search_method: SearchMethod
    index: str = None
    body: Union[dict, list]
    parameters: dict = None


class ElasticsearchConnector(ToucanConnector):
    type = "elasticsearch"
    data_source_model: ElasticsearchDataSource
    hosts: List[ElasticsearchHost]
    send_get_body_as: str = None

    def get_df(self, data_source: ElasticsearchDataSource) -> pd.DataFrame:
        data_source.body = nosql_apply_parameters_to_query(
            data_source.body,
            data_source.parameters
        )
        connection_params = []
        for host in self.hosts:
            parsed_url = urlparse(host.url)
            h = {"host": parsed_url.hostname}

            if parsed_url.path and parsed_url.path != "/":
                h["url_prefix"] = parsed_url.path
            if parsed_url.scheme == "https":
                h["port"] = host.port or 443
                h["use_ssl"] = True
            elif host.port:
                h["port"] = host.port

            if host.username or host.password:
                h["http_auth"] = f"{host.username}:{host.password}"
            if host.headers:
                h['headers'] = host.headers
            connection_params.append(h)

        esclient = Elasticsearch(connection_params,
                                 send_get_body_as=self.send_get_body_as)
        response = getattr(esclient, data_source.search_method)(
            index=data_source.index,
            body=data_source.body
        )

        filter = ""
        if data_source.search_method == SearchMethod.msearch:
            filter = ".responses[]"
        filter = filter + ".hits.hits[]._source"

        res = jq(filter).transform(response, multiple_output=True)
        df = json_normalize(res)
        return df
