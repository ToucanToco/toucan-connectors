from enum import Enum
from typing import List, Union

import pandas as pd
from jq import jq
from pydantic import BaseModel, FilePath, Schema, UrlStr
from requests import Session

from toucan_connectors.auth import Auth
from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


def transform_with_jq(data: object, jq_filter: str) -> list:
    data = jq(jq_filter).transform(data, multiple_output=True)

    # jq 'multiple outout': the data is already presented as a list of rows
    multiple_output = len(data) == 1 and isinstance(data[0], list)

    # another valid datastructure:  [{col1:[value, ...], col2:[value, ...]}]
    single_cols_dict = isinstance(data[0], dict) and isinstance(list(data[0].values())[0], list)

    if multiple_output or single_cols_dict:
        return data[0]

    return data


class Method(str, Enum):
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'


class Template(BaseModel):
    headers: dict = Schema(
        None,
        description='JSON object of HTTP headers to send with every HTTP request',
        examples=['{ "content-type": "application/xml" }'],
    )
    params: dict = Schema(
        None,
        description='JSON object of parameters to send in the query string of every HTTP request '
        '(e.g. "offset" and "limit" in https://www/api-aseroute/data&offset=100&limit=50)',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    json_: dict = Schema(
        None,
        alias='json',
        description='JSON object of parameters to send in the body of every HTTP request',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    proxies: dict = Schema(
        None,
        description='JSON object expressing a mapping of protocol or host to corresponding proxy',
        examples=['{"http": "foo.bar:3128", "http://host.name": "foo.bar:4012"}'],
    )


class HttpAPIDataSource(ToucanDataSource):
    url: str = Schema(
        ...,
        title='URL path',
        description='The URL path that will be appended to your baseroute URL. '
        'For example "geo/countries"',
    )
    method: Method = Schema(Method.GET, title='HTTP Method')
    headers: dict = Schema(
        None,
        description='JSON object of HTTP headers to send with every HTTP request',
        examples=['{ "content-type": "application/xml" }'],
    )
    params: dict = Schema(
        None,
        description='JSON object of parameters to send in the query string of this HTTP request '
        '(e.g. "offset" and "limit" in https://www/api-aseroute/data&offset=100&limit=50)',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    json_: dict = Schema(
        None,
        alias='json',
        description='JSON object of parameters to send in the body of every HTTP request',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    proxies: dict = Schema(
        None,
        description='JSON object expressing a mapping of protocol or host to corresponding proxy',
        examples=['{"http": "foo.bar:3128", "http://host.name": "foo.bar:4012"}'],
    )
    data: Union[str, dict] = Schema(
        None, description='JSON object to send in the body of the HTTP request'
    )
    filter: str = Schema(
        '.',
        description='You can apply filters to json response if data is nested. As we rely on a '
        'library called jq, we suggest the refer to the dedicated '
        '<a href="https://2.python-requests.org//en/master/user/authentication/">documentation</a>',
    )


class HttpAPIConnector(ToucanConnector):
    data_source_model: HttpAPIDataSource

    baseroute: UrlStr = Schema(..., title='API endpoint', description='Baseroute URL')
    cert: List[FilePath] = Schema(
        None, title='Certificate', description='File path of your certificate if any'
    )
    auth: Auth = Schema(None, title='Authentication type')
    template: Template = Schema(
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
            data_source.dict(by_alias=True), data_source.parameters
        )

        if self.template:
            template = {k: v for k, v in self.template.dict(by_alias=True).items() if v}
            for k in query.keys() & template.keys():
                if query[k]:
                    template[k].update(query[k])
                query[k] = template[k]

        return pd.DataFrame(self.do_request(query, session))
