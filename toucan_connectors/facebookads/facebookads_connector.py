from typing import Dict
from urllib.parse import urljoin

import pandas as pd
import requests
from pydantic import Field

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

from .helpers import ALLOWED_PARAMETERS_MAP, FacebookadsDataKind

API_BASE_ROUTE = 'https://graph.facebook.com/v10.0/'

API_ENDPOINTS_MAPPING = {
    FacebookadsDataKind.campaigns: 'act_{act_id}/campaigns',
    FacebookadsDataKind.ads_under_campaign: '{campaign_id}/ads',
}


class FacebookadsDataSource(ToucanDataSource):
    data_kind: FacebookadsDataKind = Field(..., description='')

    parameters: Dict = Field(
        None, description='A set parameters that will be applied against the retrieved data.'
    )

    campaign_id: str = Field(None, description='The ID of an ads campaign')

    def determine_url(self, account_id) -> str:
        format_key_mapping = {
            FacebookadsDataKind.campaigns: {'act_id': account_id},
            FacebookadsDataKind.ads_under_campaign: {'campaign_id': self.campaign_id},
        }
        return urljoin(
            API_BASE_ROUTE,
            API_ENDPOINTS_MAPPING[self.data_kind].format(**format_key_mapping[self.data_kind]),
        )

    def determine_query_params(self) -> Dict[str, str]:
        params = {}
        allowed_parameters = ALLOWED_PARAMETERS_MAP[self.data_kind]

        for k, v in self.parameters.items():
            if k in allowed_parameters:
                params[k] = v

        return params


class FacebookadsConnector(ToucanConnector):
    data_source_model: FacebookadsDataSource

    token: str = Field(..., description='A token associated to your facebook app')
    account_id: str = Field(..., description='The ID of your facebook account')

    def _retrieve_data(self, data_source: FacebookadsDataSource) -> pd.DataFrame:
        url = data_source.determine_url(account_id=self.account_id)
        res = requests.get(
            url, params={**data_source.determine_query_params(), 'access_token': self.token}
        )

        return pd.DataFrame(res.json().get('data'))
