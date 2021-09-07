import os
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

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

from .enums import FacebookAdsDataKind
from .helpers import has_next_page

API_BASE_ROUTE = 'https://graph.facebook.com/v10.0/'

API_ENDPOINTS_MAPPING = {
    FacebookAdsDataKind.campaigns: 'act_{act_id}/campaigns',
    FacebookAdsDataKind.ads_under_campaign: '{campaign_id}/ads',
    FacebookAdsDataKind.all_ads: 'act_{act_id}/ads',
    FacebookAdsDataKind.insights: 'act_{act_id}/insights',
}

AUTHORIZATION_URL = 'https://www.facebook.com/v10.0/dialog/oauth'
SCOPES = 'ads_read'
TOKEN_URL = 'https://graph.facebook.com/v10.0/oauth/access_token'


class FacebookAdsDataSource(ToucanDataSource):
    data_kind: FacebookAdsDataKind = Field(..., description='')

    parameters: Dict = Field(
        None, description='A set parameters that will be applied against the retrieved data.'
    )

    data_fields: str = Field(
        None,
        description=(
            "A string of comma-separated fields, those fields are listed <a href='https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group'>for campaigns</a>, "
            "<a href='https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/ads/'>for ads under a specific campaign</a> "
            "and <a href='https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/ads/'>for all the ads under a specific account"
        ),
    )

    def determine_url(self) -> str:
        format_key_mapping = {
            FacebookAdsDataKind.campaigns: {'act_id': self.parameters.get('account_id')},
            FacebookAdsDataKind.all_ads: {'act_id': self.parameters.get('account_id')},
            FacebookAdsDataKind.ads_under_campaign: {
                'campaign_id': self.parameters.get('campaign_id')
            },
            FacebookAdsDataKind.insights: {'act_id': self.parameters.get('account_id')},
        }
        return urljoin(
            API_BASE_ROUTE,
            API_ENDPOINTS_MAPPING[self.data_kind].format(**format_key_mapping[self.data_kind]),
        )

    def determine_query_params(self) -> Dict[str, str]:
        params = {}

        if self.data_fields:
            params['fields'] = self.data_fields

        for k, v in self.parameters.items():
            params[k] = v

        return params


class FacebookAdsConnector(ToucanConnector):
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]
    _oauth_trigger = 'instance'
    data_source_model: FacebookAdsDataSource
    oauth2_version = Field('1', **{'ui.hidden': True})
    _oauth2_connector: OAuth2Connector = PrivateAttr()

    def __init__(self, **kwargs) -> None:
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        self._oauth2_connector = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=SCOPES,
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
        In the Facebook Ads oAuth2 authentication process, client_id & client_secret
        must be sent in the body of the request so we have to set them in
        the mother class. This way they'll be added to her get_access_token method
        """
        return self._oauth2_connector.retrieve_tokens(authorization_response)

    def build_authorization_url(self, **kwargs):
        return self._oauth2_connector.build_authorization_url(**kwargs)

    def _get_access_token(self):
        return self._oauth2_connector.get_access_token()

    @staticmethod
    def _handle_pagination(url: str, params: Dict) -> List[Dict]:
        response = None
        response_data = None
        data = []

        while not response or has_next_page(response_data):
            response = requests.get(url, params=params)
            response.raise_for_status()

            response_data = response.json()
            if len(response_data.get('data')):
                data.extend(response_data.get('data'))

        return data

    def _retrieve_data(self, data_source: FacebookAdsDataSource) -> pd.DataFrame:
        url = data_source.determine_url()
        data = self._handle_pagination(
            url, {**data_source.determine_query_params(), 'access_token': self._get_access_token()}
        )
        return pd.DataFrame(data)
