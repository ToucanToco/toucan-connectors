import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from pydantic import Field, PrivateAttr

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

from .enums import HubspotDataset
from .helpers import has_next_page

AUTHORIZATION_URL: str = 'https://app.hubspot.com/oauth/authorize'
SCOPE: str = 'oauth contacts content forms business-intelligence e-commerce'
TOKEN_URL: str = 'https://api.hubapi.com/oauth/v1/token'
HUBSPOT_ENDPOINTS: dict = {
    'contacts': {
        'url': ' https://api.hubapi.com/contacts/v1/lists/all/contacts/all',
        # 'url': 'https://api.hubapi.com/crm/v3/objects/contacts',
        'legacy': False,
    },
    'companies': {
        'url': 'https://api.hubapi.com/companies/v2/companies/paged',
        # 'url': 'https://api.hubapi.com/crm/v3/objects/companies',
        'legacy': False,
    },
    'deals': {
        'url': 'https://api.hubapi.com/deals/v1/deal/paged',
        # 'url': 'https://api.hubapi.com/crm/v3/objects/deals',
        'legacy': False,
    },
    'products': {
        'url': 'https://api.hubapi.com/crm-objects/v1/objects/products/paged',
        # 'url': 'https://api.hubapi.com/crm/v3/objects/products',
        'legacy': False,
        'sub_name': 'objects',
    },
    # 'web-analytics': {
    #     'url': 'https://api.hubapi.com/reports/v2/events',
    #     # 'url': 'https://api.hubapi.com/events/v3/events',
    #     'legacy': False,
    # },
    # https://legacydocs.hubspot.com/docs/methods/email/get_events?_ga=2.71868499.1363348269.1614853210-1638453014.16134
    # 'emails-events': {
    #     'url': 'https://api.hubapi.com/email/public/v1/events',
    #     'legacy': True,
    #     'results_key': 'events',
    #     'paging_key': 'offset',
    # },
}


class HubspotConnectorException(Exception):
    """Custom exception for Hubspot"""


class HubspotDataSource(ToucanDataSource):
    dataset: HubspotDataset = 'contacts'
    parameters: Dict = Field(None)


class HubspotConnector(ToucanConnector):
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]
    _oauth_trigger = 'instance'
    oauth2_version = Field('1', **{'ui.hidden': True})
    data_source_model: HubspotDataSource
    _oauth2_connector: OAuth2Connector = PrivateAttr()

    def __init__(self, **kwargs) -> None:
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        self._oauth2_connector = OAuth2Connector(
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
        return self._oauth2_connector.retrieve_tokens(authorization_response)

    def build_authorization_url(self, **kwargs):
        return self._oauth2_connector.build_authorization_url(**kwargs)

    def _get_access_token(self):
        return self._oauth2_connector.get_access_token()

    def _handle_pagination(
        self,
        endpoint_info: Dict[str, Any],
        dataset_name: str,
        query_params: Dict[str, str],
        headers,
    ) -> List:
        url: str = endpoint_info['url']
        name: Optional[str] = (
            endpoint_info['sub_name'] if 'sub_name' in endpoint_info else dataset_name
        )
        response = None
        res = None
        data: List = []

        next_page_exists = has_next_page

        index = 1
        while not response or next_page_exists(res):
            if res:
                query_params['after'] = res['paging']['next']['after']

            response = requests.get(url, params=query_params, headers=headers)
            # throw if the request's status is not 200
            response.raise_for_status()
            res = response.json()
            # Flatten the results
            results = res.get(name)
            if results is None:
                raise HubspotConnectorException(f'Impossible to retrieve data for {name}')
            if results:
                for r in results:
                    data.append(r)
            index += 1
        return data

    def _retrieve_data(self, data_source: HubspotDataSource) -> pd.DataFrame:
        headers = {'authorization': f'Bearer {self._get_access_token()}'}
        try:
            query_params = {}

            # The webanalytics endpoint requires an objectType query param
            data = self._handle_pagination(
                HUBSPOT_ENDPOINTS[data_source.dataset], data_source.dataset, query_params, headers
            )
            return pd.json_normalize(data)
        except Exception as e:
            raise HubspotConnectorException(f'retrieve_data failed with: {str(e)}')
