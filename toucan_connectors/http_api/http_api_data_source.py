from enum import Enum
from typing import Any

from pydantic import Field
from pydantic.json_schema import DEFAULT_REF_TEMPLATE, GenerateJsonSchema, JsonSchemaMode

from toucan_connectors import ToucanDataSource
from toucan_connectors.common import (
    FilterSchema,
    XpathSchema,
)
from toucan_connectors.http_api.pagination_configs import (
    HttpPaginationConfig,
    PaginationConfig,
)


class Method(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"


class HttpAPIDataSource(ToucanDataSource):
    url: str = Field(
        ...,
        title="Endpoint URL",
        description='The URL path that will be appended to your baseroute URL. For example "geo/countries"',
    )
    method: Method = Field(Method.GET, title="HTTP Method")
    headers: dict | None = Field(
        None,
        description="You can also setup headers in the Template section of your Connector see: <br/>"
        "https://docs.toucantoco.com/concepteur/tutorials/connectors/3-http-connector.html#template",
        examples=['{ "content-type": "application/xml" }'],
    )
    params: dict | None = Field(
        None,
        title="URL params",
        description="JSON object of parameters to send in the query string of this HTTP request "
        '(e.g. "offset" and "limit" in https://www/api-aseroute/data&offset=100&limit=50)',
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    json_: dict | None = Field(
        None,
        alias="json",
        title="Body",
        description="JSON object of parameters to send in the body of every HTTP request",
        examples=['{ "offset": 100, "limit": 50 }'],
    )
    proxies: dict | None = Field(
        None,
        description="JSON object expressing a mapping of protocol or host to corresponding proxy",
        examples=['{"http": "foo.bar:3128", "http://host.name": "foo.bar:4012"}'],
    )
    data: str | dict | None = Field(None, description="JSON object to send in the body of the HTTP request")
    xpath: str = XpathSchema
    filter: str = FilterSchema
    flatten_column: str | None = Field(None, description="Column containing nested rows")
    http_pagination_config: HttpPaginationConfig | None = Field(
        None, title="Pagination configuration", discriminator="kind"
    )

    @classmethod
    def model_json_schema(
        cls,
        by_alias: bool = True,
        ref_template: str = DEFAULT_REF_TEMPLATE,
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation",
    ) -> dict[str, Any]:
        schema = super().model_json_schema(
            by_alias=by_alias,
            ref_template=ref_template,
            schema_generator=schema_generator,
            mode=mode,
        )
        keys = schema["properties"].keys()
        last_keys = [
            "proxies",
            "flatten_column",
            "data",
            "xpath",
            "filter",
            "validation",
        ]
        new_keys = [k for k in keys if k not in last_keys] + last_keys
        schema["properties"] = {k: schema["properties"].get(k) for k in new_keys}
        return schema


def apply_pagination_to_data_source(
    data_source: HttpAPIDataSource, pagination_config: PaginationConfig
) -> HttpAPIDataSource:
    """Apply http pagination config to its parameters"""
    updates = pagination_config.plan_pagination_updates_to_data_source(request_params=data_source.params)
    return data_source.model_copy(update=updates)
