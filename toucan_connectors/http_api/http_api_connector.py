import json
from enum import Enum
from logging import getLogger
from typing import TYPE_CHECKING, Any

from pydantic import AnyHttpUrl, BaseModel, Field, FilePath

from toucan_connectors.http_api.authentication_configs import HttpAuthenticationConfig
from toucan_connectors.http_api.http_api_data_source import HttpAPIDataSource, apply_pagination_to_data_source

try:
    from xml.etree.ElementTree import ParseError, fromstring, tostring

    import pandas as pd
    from authlib.common.security import generate_token  # noqa: F401
    from requests import Session
    from requests.exceptions import HTTPError
    from xmltodict import parse

    from toucan_connectors.http_api.pagination_configs import (
        NoopPaginationConfig,
        PaginationConfig,
        extract_pagination_info_from_result,
    )

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from toucan_connectors.auth import Auth
from toucan_connectors.common import (
    nosql_apply_parameters_to_query,
    transform_with_jq,
)
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.utils.json_to_table import json_to_table

if TYPE_CHECKING:
    from requests.exceptions import HTTPError

TOO_MANY_REQUESTS = 429
_LOGGER = getLogger(__name__)


class HttpAPIConnectorError(Exception):
    """Raised when an error occurs while fetching data from an HTTP API"""

    def __init__(self, message: str, original_exc: "HTTPError") -> None:
        super().__init__(message)
        self.original_exc = original_exc


class ResponseType(str, Enum):
    json = "json"
    xml = "xml"


class Template(BaseModel):
    headers: dict | None = Field(
        None,
        description="JSON object of HTTP headers to send with every HTTP request",
        examples=['{ "content-type": "application/xml" }'],
    )
    params: dict | None = Field(
        None,
        description="JSON object of parameters to send in the query string of every HTTP request "
        '(e.g. "offset" and "limit" in https://www/api-aseroute/data&offset=100&limit=50)',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    json_: dict | None = Field(
        None,
        alias="json",
        description="JSON object of parameters to send in the body of every HTTP request",
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    proxies: dict | None = Field(
        None,
        description="JSON object expressing a mapping of protocol or host to corresponding proxy",
        examples=['{"http": "foo.bar:3128", "http://host.name": "foo.bar:4012"}'],
    )


class HttpAPIConnector(ToucanConnector, data_source_model=HttpAPIDataSource):
    responsetype: ResponseType = Field(ResponseType.json, title="Content-type of response")
    baseroute: AnyHttpUrl = Field(..., title="Baseroute URL", description="Baseroute URL")
    cert: list[FilePath] | None = Field(None, title="Certificate", description="File path of your certificate if any")
    auth: Auth | None = Field(
        None,
        title="Authentication type",
        deprecated=True,
        description="Deprecated authentication config. Please use 'Authentication' section.",
    )  # Deprecated

    authentication: HttpAuthenticationConfig | None = Field(
        None, title="Authentication", discriminator="kind", description="Authentication configuration section"
    )

    template: Template | None = Field(
        None,
        description="You can provide a custom template that will be used for every HTTP request",
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
        xpath = query["xpath"]
        available_params = ["url", "method", "params", "data", "json", "headers", "proxies"]
        query = {k: v for k, v in query.items() if k in available_params}
        query["url"] = "/".join([str(self.baseroute).rstrip("/"), query["url"].lstrip("/")])

        if self.cert:
            # `cert` is a list of PosixPath. `request` needs a list of strings for certificates
            query["cert"] = [str(c) for c in self.cert]

        _LOGGER.debug(f">> Request:  method={query.get('method')} url={query.get('url')}")
        res = session.request(**query)
        _LOGGER.debug(f"<< Response: status_code={res.status_code} reason={res.reason}")

        res.raise_for_status()

        if self.responsetype == "xml":
            try:
                data = fromstring(res.content)  # noqa: S314
                data = parse(tostring(data.find(xpath), method="xml"), attr_prefix="")
            except ParseError:
                _LOGGER.error(f"Could not decode {res.content!r}")
                raise

        else:
            try:
                data = res.json()
            except ValueError:
                _LOGGER.error("Could not decode content using response.json()")
                try:
                    # sometimes when the content is too big res.json() fails but json.loads works
                    data = json.loads(res.content)
                except ValueError:
                    _LOGGER.error(
                        f"Cannot decode response content from query: method={query.get('method')} url={query.get('url')} response_status_code={res.status_code} response_reason=${res.reason}"  # noqa: E501
                    )
                    raise
        return data

    def perform_requests(self, data_source: HttpAPIDataSource, session: "Session") -> list[Any]:
        results = []
        # Extract first http_pagination_config from data_source
        pagination_config: PaginationConfig | None = data_source.http_pagination_config or NoopPaginationConfig()
        while pagination_config is not None:
            data_source = apply_pagination_to_data_source(data_source, pagination_config)
            query = self._render_query(data_source)
            jq_filter = query["filter"]
            query.pop("http_pagination_config")
            # Retrieve data
            try:
                raw_result = self.do_request(query, session)
            except HTTPError as exc:
                if whitelisted_status_codes := pagination_config.get_error_status_whitelist():
                    if exc.response.status_code in whitelisted_status_codes:
                        # If a whitelisted error occurs, we want to stop paginated data retrieving iteration
                        break
                    else:
                        raise
                else:
                    raise
            # Parse retrieved data with JQ filter
            try:
                parsed_result = transform_with_jq(raw_result, jq_filter)
            except ValueError:
                _LOGGER.error(f"Could not transform {raw_result} using {jq_filter}")
                raise
            # Prepare next pagination config
            parsed_pagination_info = None
            # Extract pagination metadata from api response if needed
            if jq_pagination_filter := pagination_config.get_pagination_info_filter():
                parsed_pagination_info = extract_pagination_info_from_result(raw_result, jq_pagination_filter)
            pagination_config = pagination_config.get_next_pagination_config(
                result=parsed_result, pagination_info=parsed_pagination_info
            )
            results.append(parsed_result)
        return results

    def _retrieve_data(self, data_source: HttpAPIDataSource) -> "pd.DataFrame":
        if self.authentication:
            # New authentication has priority
            session = self.authentication.authenticate_session()
        elif self.auth:
            session = self.auth.get_session()
        else:
            session = Session()
        # Try retrieve dataset
        try:
            results = self.perform_requests(
                data_source=data_source,
                session=session,
            )
            dfs = [pd.DataFrame(result) for result in results]
        except HTTPError as exc:
            if exc.response.status_code == TOO_MANY_REQUESTS:
                raise HttpAPIConnectorError(
                    message="Failed to retrieve data: the connector tried to perform too many requests."
                    " Please check your API call limitations.",
                    original_exc=exc,
                ) from exc
            else:
                raise
        if data_source.flatten_column:
            dfs = [json_to_table(df, columns=[data_source.flatten_column]) for df in dfs]
        return pd.concat(dfs, ignore_index=True)

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
        del query["parameters"]
        return query
