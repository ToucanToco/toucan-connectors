import json
from copy import deepcopy
from enum import Enum
from logging import getLogger
from typing import Any
from urllib.parse import quote, urlparse

try:
    import pandas as pd
    from elasticsearch import Elasticsearch
    from pandas import json_normalize

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False
from pydantic import BaseModel, Field

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import PlainJsonSecretStr, ToucanConnector, ToucanDataSource


def _is_branch_list(val):
    res = False
    if isinstance(val, dict):
        for _k, v in val.items():
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
            new_parent = f"{parent}_{k}" if parent else k
            neighbours = _flatten_aggregations(v, new_parent, neighbours)

        if not branch_l:
            return neighbours
        else:
            res = []
            for k, v in branch_l.items():
                new_parent = f"{parent}_{k}" if parent else k
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
    if "aggregations" in response:
        res = _flatten_aggregations(response["aggregations"])
        if isinstance(res, dict):
            res = [res]
    else:
        res = [elt["_source"] for elt in response["hits"]["hits"]]
    return res


class ElasticsearchHost(BaseModel):
    url: str
    port: int | None = None
    scheme: str | None = None
    username: str | None = None
    password: PlainJsonSecretStr | None = Field(None, description="Your login password")
    headers: dict | None = None


class SearchMethod(str, Enum):
    search = "search"
    msearch = "msearch"


class ElasticsearchDataSource(ToucanDataSource):
    search_method: SearchMethod = Field(SearchMethod.search, title="Search method")
    index: str | None = Field(None, title="Index")
    body: dict[str, Any] | list[Any] = Field(  # type:ignore[call-overload]
        default_factory=dict, description="Body of elasticsearch query", widget="json"
    )


class ElasticsearchConnector(ToucanConnector, data_source_model=ElasticsearchDataSource):
    hosts: list[ElasticsearchHost]
    es_version: int = 8

    def _retrieve_data(self, data_source: ElasticsearchDataSource) -> "pd.DataFrame":
        data_source.body = nosql_apply_parameters_to_query(data_source.body, data_source.parameters)
        connection_params = []
        basic_auth: tuple[str, str] | None = None
        headers: dict[str, str] | None = None
        for host in self.hosts:
            parsed_url = urlparse(host.url)
            h: dict[str, Any] = {"host": parsed_url.hostname}

            if parsed_url.path and parsed_url.path != "/":
                h["url_prefix"] = parsed_url.path
            if parsed_url.scheme == "https":
                h["port"] = host.port or 443
                h["scheme"] = parsed_url.scheme
            elif host.port:
                h["port"] = host.port
                h["scheme"] = parsed_url.scheme

            if host.username or host.password:
                password = host.password.get_secret_value() if host.password is not None else ""
                username = host.username or ""
                # the "username:password" form does not work with esclient 9
                basic_auth = (username, password)
            if host.headers:
                headers = host.headers
            connection_params.append(h)

        request_headers = {
            "accept": f"application/vnd.elasticsearch+json; compatible-with={self.es_version}",
            "content-type": f"application/vnd.elasticsearch+json; compatible-with={self.es_version}",
        }

        if headers:
            request_headers.update({k.lower(): v for k, v in headers.items()})
        kwargs: dict[str, Any] = {"basic_auth": basic_auth, "headers": request_headers}
        esclient = Elasticsearch(connection_params, **kwargs)

        # We need to set this flag as some customers force auth and refuse the connection if no auth
        # header is present. Elasticsearch-py accepts 401/403s
        # but not connection errors. In consequence, we set the flag to True, which means that we
        # couldn't figure out whether we are talking to Elasticsearch or not due to an auth error.
        # If we are indeed not talking to Elasticsearch, the query will fail later on.
        esclient._verified_elasticsearch = True

        quoted_index = quote(data_source.index or "", ",*")
        path = f"/{quoted_index}/_{data_source.search_method.value}"
        if data_source.search_method == SearchMethod.search:
            body: Any = data_source.body
        else:
            # ndjson must be terminated by a newline
            body = "\n".join(json.dumps(item) for item in data_source.body) + "\n"

        response = esclient.perform_request(
            "POST", path, body=body, headers=request_headers, endpoint_id=data_source.search_method.value
        )

        if data_source.search_method == SearchMethod.msearch:
            assert isinstance(data_source.body, list)
            res = []
            # Body alternate index and query `[index, query, index, query...]`
            queries = data_source.body[1::2]
            for _query, data in zip(queries, response["responses"], strict=False):
                res += _read_response(data)
        else:
            res = _read_response(response)

        df = json_normalize(res)
        return df
