from collections import Sequence
from typing import Any, Dict, Type

import pandas as pd
from pydantic import Field, create_model
from requests import Session
from toucan_data_sdk.utils.postprocess.json_to_table import json_to_table
from zeep import Client
from zeep.transports import Transport

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum


class SoapDataSource(ToucanDataSource):
    method: str = Field(None, title='Method', description='Name of the webservice method to use')
    parameters: dict = Field(
        None, title='Service Parameters', description='Parameters to pass to the called service'
    )
    flatten_column: str = Field(None, description='Column containing nested rows')

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['SoapDataSource']) -> None:
            keys = schema['properties'].keys()
            prio_keys = ['domain', 'method', 'parameters', 'flatten_column']
            new_keys = prio_keys + [k for k in keys if k not in prio_keys]
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}

    @classmethod
    def _get_methods_docs(cls, client):
        # Returns a list of available methods
        docs = {}
        for m in dir(client.service):
            if not m.startswith('_'):
                docs[m] = getattr(client.service, m).__doc__
        return docs

    @classmethod
    def get_form(cls, connector: 'SoapConnector', current_config):
        constraints = {}
        client = connector.create_client()
        methods_docs = cls._get_methods_docs(client)
        constraints['method'] = strlist_to_enum('method', list(methods_docs.keys()))

        res = create_model('FormSchema', **constraints, __base__=cls).schema()
        res['properties']['parameters'][
            'description'
        ] = f'Services documentation: <br> {"<br>".join(list(methods_docs.values()))}'
        return res


class SoapConnector(ToucanConnector):
    data_source_model: SoapDataSource
    headers: dict = Field(
        None,
        description='JSON object of HTTP headers to send with every HTTP request',
        examples=['{ "content-type": "application/xml" }'],
    )
    endpoint: str = Field(
        ..., title='WSDL Endpoint', description='The URL where the WSDL file is located'
    )

    def create_client(self) -> Client:
        session = Session()
        if self.headers:
            session.headers.update(self.headers)
        return Client(self.endpoint, transport=Transport(session=session))

    def _retrieve_data(self, data_source: SoapDataSource) -> pd.DataFrame:
        # Instantiate the SOAP client

        client = self.create_client()
        # Force the casting of response
        response = getattr(client.service, data_source.method)(**data_source.parameters)

        if isinstance(response, Sequence):
            result = pd.DataFrame(response)
        else:
            result = pd.DataFrame({'response': response}, index=[0])

        if data_source.flatten_column:
            return json_to_table(result, columns=[data_source.flatten_column])
        return result
