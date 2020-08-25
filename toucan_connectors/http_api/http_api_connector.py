from enum import Enum
from typing import List, Union

import pandas as pd
from pydantic import AnyHttpUrl, BaseModel, Field, FilePath
from requests import Session

from toucan_connectors.auth import Auth
from toucan_connectors.common import (
    FilterSchema,
    nosql_apply_parameters_to_query,
    transform_with_jq,
)
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


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
        description='JSON object of HTTP headers to send with every HTTP request',
        examples=['{ "content-type": "application/xml" }'],
    )
    params: dict = Field(
        None,
        description='JSON object of parameters to send in the query string of this HTTP request '
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
    data: Union[str, dict] = Field(
        None, description='JSON object to send in the body of the HTTP request'
    )
    filter: str = FilterSchema


class HttpAPIConnector(ToucanConnector):
    data_source_model: HttpAPIDataSource

    baseroute: AnyHttpUrl = Field(..., title='Baseroute URL', description='Baseroute URL')
    cert: List[FilePath] = Field(
        None, title='Certificate', description='File path of your certificate if any'
    )
    auth: Auth = Field(None, title='Authentication type')
    template: Template = Field(
        None,
        description='You can provide a custom template that will be used for every HTTP request',
    )

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
            HttpAPIConnector.logger.error(f'Could not decode {res.content!r}')
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
            data_source.dict(by_alias=True), data_source.parameters, handle_errors=True
        )

        if self.template:
            template = {k: v for k, v in self.template.dict(by_alias=True).items() if v}
            for k in query.keys() & template.keys():
                if query[k]:
                    template[k].update(query[k])
                query[k] = template[k]

        return pd.DataFrame(self.do_request(query, session))
