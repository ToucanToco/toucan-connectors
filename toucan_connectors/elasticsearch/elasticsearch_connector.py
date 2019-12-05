from copy import deepcopy
from enum import Enum
from typing import List, Union
from urllib.parse import urlparse

import pandas as pd
from elasticsearch import Elasticsearch
from pandas.io.json import json_normalize
from pydantic import BaseModel

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


def _is_branch_list(val):
    res = False
    if isinstance(val, dict):
        for k, v in val.items():
            if _is_branch_list(v):
                res = True
                break
    elif isinstance(val, list):
        res = True
    return res


def _flatten_aggregations(data, parent=None, neighbours=None):
    """
    Read `aggregations` block in data.
    Example
      Input data:
      ```
        aggregation: {
            field1 : {
                buckets: [
                    {key: 'name1', count: 5},
                    {key: 'name2', count: 10}
                ]
            },
            field2: 5,
            field3 : {
                buckets: [
                    {key: 'name3', count: 7}
                ]
            },
        }
      ```
         Result:
      ```
      [{'field2': 5, 'field1_bucket_key': 'name1', 'field1_bucket_count': 5},
      {'field2': 5, 'field1_bucket_key': 'name2', 'field1_bucket_count': 10},
      {'field2': 5, 'field3_bucket_key': 'name3', 'field3_bucket_count': 7}]
      ```
    """
    if not neighbours:
        neighbours = {}
    if isinstance(data, dict):
        branch_l = {}
        for k, v in deepcopy(data).items():
            if _is_branch_list(v):
                branch_l[k] = v
                data.pop(k)

        for k, v in data.items():
            new_parent = f'{parent}_{k}' if parent else k
            neighbours = _flatten_aggregations(v, new_parent, neighbours)

        if not branch_l:
            return neighbours
        else:
            res = []
            for k, v in branch_l.items():
                new_parent = f'{parent}_{k}' if parent else k
                if isinstance(v, list):  # buckets
                    new_list = []
                    for elt in v:
                        new_elt = _flatten_aggregations(elt, new_parent, neighbours)
                        if isinstance(new_elt, list):
                            new_list += new_elt
                        else:
                            new_list.append(new_elt)
                    res += new_list
                else:
                    res += _flatten_aggregations(v, new_parent, neighbours)
            return res
    else:
        return {**{parent: data}, **neighbours}


def _read_response(response):
    if 'aggregations' in response:
        res = _flatten_aggregations(response['aggregations'])
        if isinstance(res, dict):
            res = [res]
    else:
        res = [elt['_source'] for elt in response['hits']['hits']]
    return res


class ElasticsearchHost(BaseModel):
    url: str
    port: int = None
    username: str = None
    password: str = None
    headers: dict = None


class SearchMethod(str, Enum):
    search = 'search'
    msearch = 'msearch'


class ElasticsearchDataSource(ToucanDataSource):
    search_method: SearchMethod
    index: str = None
    body: Union[dict, list]


class ElasticsearchConnector(ToucanConnector):
    data_source_model: ElasticsearchDataSource
    hosts: List[ElasticsearchHost]
    send_get_body_as: str = None

    def _retrieve_data(self, data_source: ElasticsearchDataSource) -> pd.DataFrame:
        data_source.body = nosql_apply_parameters_to_query(data_source.body, data_source.parameters)
        connection_params = []
        for host in self.hosts:
            parsed_url = urlparse(host.url)
            h = {'host': parsed_url.hostname}

            if parsed_url.path and parsed_url.path != '/':
                h['url_prefix'] = parsed_url.path
            if parsed_url.scheme == 'https':
                h['port'] = host.port or 443
                h['use_ssl'] = True
            elif host.port:
                h['port'] = host.port

            if host.username or host.password:
                h['http_auth'] = f'{host.username}:{host.password}'
            if host.headers:
                h['headers'] = host.headers
            connection_params.append(h)

        esclient = Elasticsearch(connection_params, send_get_body_as=self.send_get_body_as)
        response = getattr(esclient, data_source.search_method)(
            index=data_source.index, body=data_source.body
        )

        if data_source.search_method == SearchMethod.msearch:
            res = []
            # Body alternate index and query `[index, query, index, query...]`
            queries = data_source.body[1::2]
            for query, data in zip(queries, response['responses']):
                res += _read_response(data)
        else:
            res = _read_response(response)

        df = json_normalize(res)
        return df
