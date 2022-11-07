import json
from ast import literal_eval
from enum import Enum
from typing import Any, Dict, List, Type, Union
from xml.etree.ElementTree import ParseError, fromstring, tostring

import pandas as pd
from pydantic import AnyHttpUrl, BaseModel, Field, FilePath
from requests import Session
from toucan_data_sdk.utils.postprocess.json_to_table import json_to_table
from xmltodict import parse

from toucan_connectors.auth import Auth
from toucan_connectors.common import (
    FilterSchema,
    XpathSchema,
    nosql_apply_parameters_to_query,
    transform_with_jq,
)
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class ResponseType(str, Enum):
    json = 'json'
    xml = 'xml'


class Method(str, Enum):
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'


class Template(BaseModel):
    headers: dict = Field(
        None,
        description='JSON object of HTTP headers to send with every HTTP request',
        examples=['{ "content-type": "application/xml" }'],
    )
    params: dict = Field(
        None,
        description='JSON object of parameters to send in the query string of every HTTP request '
        '(e.g. "offset" and "limit" in https://www/api-aseroute/data&offset=100&limit=50)',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    json_: dict = Field(
        None,
        alias='json',
        description='JSON object of parameters to send in the body of every HTTP request',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    proxies: dict = Field(
        None,
        description='JSON object expressing a mapping of protocol or host to corresponding proxy',
        examples=['{"http": "foo.bar:3128", "http://host.name": "foo.bar:4012"}'],
    )


class CustomPagination(BaseModel):
    """
    For example :
        {
            "keys_values": ["page=20", "limit=100"],
        }
    """

    keys_values: list[str] = Field(
        None,
        description='List of keys/values for the custom pagination in order',
        examples=['[offset = 10, limit = 200, filter = created:gt:2020]'],
    )


class RestApiLevel4(BaseModel):
    """
    For example, from each request,
    get the key:chain to access the next future page:
        {
            "_limits": {
                "next": {
                    "href": "http://127.0.0.1:3000/api/v1?page=2&limit=12"
                }
            }
        }

    next_page -> _limits.next.href
    """

    JQ_FILTER: str = Field(
        None,
        description='The jq filter that represent the path to access the value of the next page',
        examples=['_limits.next.href'],
    )


class GraphQL(BaseModel):
    """
    On a GraphQL pagination
    """

    QL_FILTER: str = Field(
        None,
        description='The QL filter that represent the filter for the graphQl',
        examples=['pageInfo{ page { value: 10 } }'],
    )


class PaginationType(BaseModel):
    """
    We can have 3 type of supported pagination
    The custom one that will be defined by the AB him(her)self
    as page/limit, offset/limit or start_id|after_id/limit

    or
    The rest_api_level4 pagination where we just have the link of the next_page
    we need to access

    or
    The graphQl pagination, for this one, no need of a model since the JQ will
    be sent as a string to the target
    """

    custom: CustomPagination = Field(
        None,
        title='Custom Pagination',
        description='For the custom pagination, you can have : '
        '- "offset" if the base API is an offset/limit pagination, '
        '- "page" if the base API is a Page Offset pagination, '
        '- "filter" or "where" if the base API is a KeySet pagination, '
        '- "after_id" or "start_id" if the base API is an Seek pagination, ',
    )

    rest_api_level4: RestApiLevel4 = Field(
        None,
        title='Rest API LEVEL 4 Pagination',
        description='For the rest API LEVEL 4, you can specify the next_page'
        'jq key to access future pages',
    )

    graph_ql: GraphQL = Field(
        None,
        title='GraphQL pagination like',
        description='For the GraphQL pagination, you can specify the JQ as string',
    )


class HttpAPIDataSource(ToucanDataSource):
    url: str = Field(
        ...,
        title='Endpoint URL',
        description='The URL path that will be appended to your baseroute URL. '
        'For example "geo/countries"',
    )
    method: Method = Field(Method.GET, title='HTTP Method')
    headers: dict = Field(
        None,
        description='You can also setup headers in the Template section of your Connector see: <br/>'
        'https://docs.toucantoco.com/concepteur/tutorials/connectors/3-http-connector.html#template',
        examples=['{ "content-type": "application/xml" }'],
    )
    pagination_type: PaginationType = Field(
        ...,
        title='Pagination Type',
        description='',
    )
    params: dict = Field(
        None,
        title='URL params',
        description='JSON object of parameters to send in the query string of this HTTP request '
        '(e.g. "valueOf" in https://www/api-baseroute/data&valueOf=test)',
        examples=['{"valueOf": "test"}'],
    )
    json_: dict = Field(
        None,
        alias='json',
        title='Body',
        description='JSON object of parameters to send in the body of every HTTP request',
        examples=['{ "payload": [], "body": {} }'],
    )
    proxies: dict = Field(
        None,
        description='JSON object expressing a mapping of protocol or host to corresponding proxy',
        examples=['{"http": "foo.bar:3128", "http://host.name": "foo.bar:4012"}'],
    )
    data: Union[str, dict] = Field(
        None, description='JSON object to send in the body of the HTTP request'
    )
    xpath: str = XpathSchema
    filter: str = FilterSchema
    flatten_column: str = Field(None, description='Column containing nested rows')

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['HttpAPIDataSource']) -> None:
            keys = schema['properties'].keys()
            last_keys = [
                'proxies',
                'flatten_column',
                'data',
                'xpath',
                'filter',
                'validation',
            ]
            new_keys = [k for k in keys if k not in last_keys] + last_keys
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}


class HttpAPIConnector(ToucanConnector):
    data_source_model: HttpAPIDataSource
    responsetype: ResponseType = Field(ResponseType.json, title='Content-type of response')
    baseroute: AnyHttpUrl = Field(..., title='Baseroute URL', description='Baseroute URL')
    cert: List[FilePath] = Field(
        None, title='Certificate', description='File path of your certificate if any'
    )
    auth: Auth = Field(None, title='Authentication type')
    template: Template = Field(
        None,
        description='You can provide a custom template that will be used for every HTTP request',
    )

    # def _extract_pagination_values(self, query: dict) -> dict:
    #     """
    #     This method just formalize the pagination keys/value depending on the
    #     type of the pagination style
    #     """

    #     query['params'] = {} if query['params'] is None else query['params']
    #     if query.get('pagination_type', None) is not None:
    #         if (custom_keys_values := query['pagination_type'].dict().get('custom', None)) is not None:
    #             for k_v in custom_keys_values:
    #                 key = k_v.split("=")[0]
    #                 value = literal_eval(k_v.split("=")[1])

    #                 query['params'] = {**query['params'], **{key: value}}

    #         if (graphQl_str := query['pagination_type'].dict().get('graphQl', None)) is not None:
    #             query['params'] = {**query['params'], **{'': value}}

    #         if (custom_keys_values := query['pagination_type'].dict().get('rest_api_level4', None)) is not None:
    #         query.pop('pagination_type', None)

    #     if query.get('limit', None) is not None:
    #         query['params'] = {**query.get('params', {}), **{'limit': query['limit']}}
    #         query.pop('limit', None)

    #     return query

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
        xpath = query['xpath']
        available_params = [
            'url',
            'method',
            'params',
            'data',
            'json',
            'headers',
            'proxies',
            'pagination_type',
        ]
        query = {k: v for k, v in query.items() if k in available_params}
        query['url'] = '/'.join([self.baseroute.rstrip('/'), query['url'].lstrip('/')])

        if self.cert:
            # `cert` is a list of PosixPath. `request` needs a list of strings for certificates
            query['cert'] = [str(c) for c in self.cert]

        # We extract and parse pagination keys/values
        # query = self._extract_pagination_values(query)  # type: ignore

        query['params'] = {} if query['params'] is None else query['params']
        if query.get('pagination_type', None) is not None:
            if (
                custom_keys_values := query['pagination_type'].dict().get('custom', None)
            ) is not None:
                for k_v in custom_keys_values:
                    key = k_v.split('=')[0]
                    value = literal_eval(k_v.split('=')[1])

                    query['params'] = {**query['params'], **{key: value}}

            if (graphQl_str := query['pagination_type'].dict().get('graphQl', None)) is not None:
                query['method'] = Method.POST
                query['data'] += graphQl_str

            # we need to handle this only after the first request... do we need
            # to handle it here or further on laputa ?
            # if (custom_keys_values := query['pagination_type'].dict().get('rest_api_level4', None)) is not None:

        res = session.request(**query)

        if self.responsetype == 'xml':
            try:
                data = fromstring(res.content)
                data = parse(tostring(data.find(xpath), method='xml'), attr_prefix='')
            except ParseError:
                HttpAPIConnector.logger.error(f'Could not decode {res.content!r}')
                raise

        else:
            try:
                data = res.json()
            except ValueError:
                HttpAPIConnector.logger.error('Could not decode content using response.json()')
                try:
                    # sometimes when the content is too big res.json() fails but json.loads works
                    data = json.loads(res.content)
                except ValueError:
                    HttpAPIConnector.logger.error('Cannot decode response content')
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

        query = self._render_query(data_source)

        res = pd.DataFrame(self.do_request(query, session))
        if data_source.flatten_column:
            return json_to_table(res, columns=[data_source.flatten_column])
        return res

    def _render_query(self, data_source):
        query = nosql_apply_parameters_to_query(
            data_source.dict(by_alias=True), data_source.parameters, handle_errors=True
        )
        if self.template:
            template = {k: v for k, v in self.template.dict(by_alias=True).items() if v}
            for k in query.keys() & template.keys():
                if query[k]:
                    template[k].update(query[k])
                query[k] = template[k]
        return query

    def _get_unique_datasource_identifier(self, data_source: ToucanDataSource) -> dict:
        query = self._render_query(data_source)
        del query['parameters']
        return query
