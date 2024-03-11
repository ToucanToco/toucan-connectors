from typing import Any

import pandas as pd
from pydantic import Field, create_model
from pydantic.json_schema import DEFAULT_REF_TEMPLATE, GenerateJsonSchema, JsonSchemaMode
from requests import Session
from toucan_data_sdk.utils.postprocess.json_to_table import json_to_table
from zeep import Client
from zeep.helpers import serialize_object
from zeep.transports import Transport

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum

from .helpers import is_dict_of_lists, is_list_response, is_nested_list


class SoapDataSource(ToucanDataSource):
    method: str = Field(None, title="Method", description="Name of the webservice method to use")
    parameters: dict = Field(None, title="Service Parameters", description="Parameters to pass to the called service")
    flatten_column: str = Field(None, description="Column containing nested rows")

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
        prio_keys = ["domain", "method", "parameters", "flatten_column"]
        new_keys = prio_keys + [k for k in keys if k not in prio_keys]
        schema["properties"] = {k: schema["properties"][k] for k in new_keys}
        return schema

    @classmethod
    def _get_methods_docs(cls, client):
        # Returns a list of available methods
        docs = {}
        for m in dir(client.service):
            if not m.startswith("_"):
                docs[m] = getattr(client.service, m).__doc__
        return docs

    @classmethod
    def get_form(cls, connector: "SoapConnector", current_config):
        constraints = {}
        client = connector.create_client()
        methods_docs = cls._get_methods_docs(client)
        constraints["method"] = strlist_to_enum("method", list(methods_docs.keys()))

        res = create_model("FormSchema", **constraints, __base__=cls).schema()
        res["properties"]["parameters"]["description"] = (
            f'Services documentation: <br> {"<br>".join(list(methods_docs.values()))}'
        )
        return res


class SoapConnector(ToucanConnector, data_source_model=SoapDataSource):
    headers: dict = Field(
        None,
        description="JSON object of HTTP headers to send with every HTTP request",
        examples=['{ "content-type": "application/xml" }'],
    )
    endpoint: str = Field(..., title="WSDL Endpoint", description="The URL where the WSDL file is located")

    def create_client(self) -> Client:
        session = Session()
        if self.headers:
            session.headers.update(self.headers)
        return Client(self.endpoint, transport=Transport(session=session))

    def _retrieve_data(self, data_source: SoapDataSource) -> pd.DataFrame:
        # Instantiate the SOAP client

        client = self.create_client()
        response = serialize_object(getattr(client.service, data_source.method)(**data_source.parameters))
        #  The connector must handle the cases where response is nested
        #  to be parsed as a tabular format
        if is_list_response(response):
            if is_nested_list(response):  # If response is like [['a', 'b',' c', 'd']]
                result = pd.DataFrame(response[0])  # Result will be pd.DataFrame(['a', 'b', 'c', 'd'])
            elif is_dict_of_lists(response):  # If response is like [{'col1':['value', 'value'], 'col2':['value',
                # 'value']}]
                result = pd.DataFrame(response[0])  # Result will be pd.DataFrame({'col1':[...], 'col2':[...]})
            else:  # Result will be directly created from response (even an empty list)
                result = pd.DataFrame(response)

        elif isinstance(response, dict):  # In case the result is only a dict
            result = pd.DataFrame(response, index=[0])  # We need to give an index to DataFrame constructor
        else:  # Occurs when the result is a scalar (e.g. an int or a str)
            result = pd.DataFrame({"response": response}, index=[0])

        return json_to_table(result, columns=[data_source.flatten_column]) if data_source.flatten_column else result
