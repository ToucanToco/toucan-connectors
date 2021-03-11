import os
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests
from pydantic import Field

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

from .enums import HubspotDataset, HubspotObjectType
from .helpers import has_next_page, has_next_page_legacy

AUTHORIZATION_URL: str = 'https://app.hubspot.com/oauth/authorize'
SCOPE: str = 'oauth contacts content forms business-intelligence e-commerce'
TOKEN_URL: str = 'https://api.hubapi.com/oauth/v1/token'
HUBSPOT_ENDPOINTS: dict = {
    'contacts': {
        'url': 'https://api.hubapi.com/crm/v3/objects/contacts',
        'legacy': False,
    },
    'companies': {
        'url': 'https://api.hubapi.com/crm/v3/objects/companies',
        'legacy': False,
    },
    'deals': {
        'url': 'https://api.hubapi.com/crm/v3/objects/deals',
        'legacy': False,
    },
    'products': {'url': 'https://api.hubapi.com/crm/v3/objects/products', 'legacy': False},
    'web-analytics': {'url': 'https://api.hubapi.com/events/v3/events', 'legacy': False},
    # https://legacydocs.hubspot.com/docs/methods/email/get_events?_ga=2.71868499.1363348269.1614853210-1638453014.16134
    'emails-events': {
        'url': 'https://api.hubapi.com/email/public/v1/events',
        'legacy': True,
        'results_key': 'events',
        'paging_key': 'offset',
    },
}


class HubspotConnectorException(Exception):
    """Custom exception for Hubspot"""


class HubspotDataSource(ToucanDataSource):
    dataset: HubspotDataset = 'contacts'
    object_type: HubspotObjectType = None
    parameters: Dict = Field(None)


class HubspotConnector(ToucanConnector):
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]

    data_source_model: HubspotDataSource

    def __init__(self, **kwargs) -> None:
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=SCOPE,
            token_url=TOKEN_URL,
            secrets_keeper=kwargs['secrets_keeper'],
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
        )

    @staticmethod
    def get_connector_secrets_form() -> ConnectorSecretsForm:
        return ConnectorSecretsForm(
            documentation_md=(Path(os.path.dirname(__file__)) / 'doc.md').read_text(),
            secrets_schema=OAuth2ConnectorConfig.schema(),
        )

    def retrieve_tokens(self, authorization_response: str):
        """
        In the Hubspot oAuth2 authentication process, client_id & client_secret
        must be sent in the body of the request so we have to set them in
        the mother class. This way they'll be added to her get_access_token method
        """
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def _get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def _handle_pagination(self, endpoint_info, query_params, headers):
        url = endpoint_info['url']
        legacy = endpoint_info['legacy']
        response = None
        res = None
        data = []

        next_page_exists = has_next_page
        if legacy:
            next_page_exists = has_next_page_legacy

        while not response or next_page_exists(res):
            if response and not legacy:
                query_params['after'] = res['paging']['next']['after']
            elif response:
                query_params[endpoint_info['paging_key']] = res[endpoint_info['paging_key']]

            response = requests.get(url, params=query_params, headers=headers)
            # throw if the request's status is not 200
            response.raise_for_status()
            res = response.json()
            # Flatten the results
            if not legacy:
                results = res.get('results')
            else:
                results = res.get(endpoint_info['results_key'])

            if results:
                for r in results:
                    data.append(r)

        return data

    def _retrieve_data(self, data_source: HubspotDataSource) -> pd.DataFrame:
        headers = {'authorization': f'Bearer {self._get_access_token()}'}
        try:
            query_params = {}

            # The webanalytics endpoint requires an objectType query param
            if data_source.object_type and data_source.dataset == HubspotDataset.webanalytics:
                query_params['objectType'] = data_source.object_type
                # Add properties if specified in parameters
                # More details are available in HubSpot's documentation: https://developers.hubspot.com/docs/api/events/web-analytics
                properties = [p for p in data_source.parameters.keys() if 'objectProperty' in p]
                for prop_name in properties:
                    query_params[prop_name] = data_source.parameters[prop_name]

            data = self._handle_pagination(
                HUBSPOT_ENDPOINTS[data_source.dataset], query_params, headers
            )

            return pd.json_normalize(data)
        except Exception as e:
            raise HubspotConnectorException(f'retrieve_data failed with: {str(e)}')
