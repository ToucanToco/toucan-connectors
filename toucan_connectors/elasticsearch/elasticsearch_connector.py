from enum import Enum
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


def flatten_aggregations(query, data, parents):
    """
    Read `aggregations` block in data.
    Example
      Input data:
      ```
        aggregation: {
            field1 : {
                buckets: [{
                    key: 'name1',
                    field2: {
                        buckets: [
                            {key: 'type1', count_document: 5},
                            {key: 'type2', count_document: 7},
                        ]
                    }
                }]
            }
        }
      ```
         Result:
      ```
      [{'field1': 'name1', 'field2': 'type1', 'count': 5},
      {'field1': 'name1', 'field2': 'type2', 'count': 7}]
      ```
    """
    key = next(iter(query))
    res = []
    for block in data[key]['buckets']:
        elt = block['key']
        if 'aggs' in query[key]:
            sub_query = query[key]['aggs']
            new_parents = {**parents, key: elt}
            res = res + flatten_aggregations(sub_query, block, new_parents)
        else:
            res.append({**parents, key: elt, 'count': block['doc_count']})
    return res


def _read_response(query, response):
    if 'aggs' in query:
        res = flatten_aggregations(query['aggs'], response['aggregations'], {})
    else:
        res = [elt['_source']for elt in response['hits']['hits']]
    return res


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
    data_source_model: ElasticsearchDataSource
    hosts: List[ElasticsearchHost]
    send_get_body_as: str = None

    def _retrieve_data(self, data_source: ElasticsearchDataSource) -> pd.DataFrame:
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

        if data_source.search_method == SearchMethod.msearch:
            res = []
            # Body alternate index and query `[index, query, index, query...]`
            queries = data_source.body[1::2]
            for query, data in zip(queries, response['responses']):
                res += _read_response(query, data)
        else:
            res = _read_response(data_source.body, response)

        df = json_normalize(res)
        return df
