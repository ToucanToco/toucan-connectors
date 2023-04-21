"""LinkedinAds connector"""
import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Type

import dateutil.parser
import pandas as pd
import requests
from pydantic import Field, PrivateAttr
from toucan_data_sdk.utils.postprocess.json_to_table import json_to_table

from toucan_connectors.common import ConnectorStatus, HttpError
from toucan_connectors.http_api.http_api_connector import Template
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

AUTHORIZATION_URL: str = 'https://www.linkedin.com/oauth/v2/authorization'
SCOPE: str = 'r_organization_social,r_ads_reporting,r_ads'
TOKEN_URL: str = 'https://www.linkedin.com/oauth/v2/accessToken'


class FinderMethod(str, Enum):
    analytics = 'analytics'
    statistics = 'statistics'


class TimeGranularity(str, Enum):
    # https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#query-parameters
    all = 'ALL'
    daily = 'DAILY'
    monthly = 'MONTHLY'
    yearly = 'YEARLY'


class NoCredentialsError(Exception):
    """Raised when no secrets available."""


class LinkedinadsDataSource(ToucanDataSource):
    """
    LinkedinAds data source class.
    """

    finder_methods: FinderMethod = Field(
        FinderMethod.analytics, title='Finder methods', description='Default: analytics'
    )
    start_date: str = Field(
        ..., title='Start date', description='Start date of the dataset. Format must be dd/mm/yyyy.'
    )
    end_date: str = Field(
        None,
        title='End date',
        description='End date of the dataset, optional & default to today. Format must be dd/mm/yyyy.',
    )
    time_granularity: TimeGranularity = Field(
        TimeGranularity.all,
        title='Time granularity',
        description='Granularity of the dataset, default all result grouped',
    )
    flatten_column: str = Field(None, description='Column containing nested rows')

    parameters: dict = Field(
        None,
        description='See https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting for more information',
    )

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['LinkedinadsDataSource']) -> None:
            keys = schema['properties'].keys()
            prio_keys = [
                'finder_methods',
                'start_date',
                'end_date',
                'time_granularity',
                'flatten_column',
                'parameters',
            ]
            new_keys = prio_keys + [k for k in keys if k not in prio_keys]
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}


class LinkedinadsConnector(ToucanConnector):
    """The LinkedinAds connector."""

    data_source_model: LinkedinadsDataSource
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[
        str
    ]  # This ID is generated & provided to the data provider during the oauth authentication process
    _baseroute = 'https://api.linkedin.com/v2/adAnalyticsV2?q='
    template: Template = Field(
        None,
        description='You can provide a custom template that will be used for every HTTP request',
    )
    _oauth_trigger = 'instance'
    oauth2_version = Field('1', **{'ui.hidden': True})
    _oauth2_connector: OAuth2Connector = PrivateAttr()

    @staticmethod
    def get_connector_secrets_form() -> ConnectorSecretsForm:
        return ConnectorSecretsForm(
            documentation_md=(Path(os.path.dirname(__file__)) / 'doc.md').read_text(),
            secrets_schema=OAuth2ConnectorConfig.schema(),
        )

    def __init__(self, **kwargs):
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        # we use __dict__ so that pydantic does not complain about the _oauth2_connector field
        self._oauth2_connector = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=SCOPE,
            token_url=TOKEN_URL,
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
            secrets_keeper=kwargs['secrets_keeper'],
        )

    def build_authorization_url(self, **kwargs):
        return self._oauth2_connector.build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self._oauth2_connector.retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self._oauth2_connector.get_access_token()

    def _retrieve_data(self, data_source: LinkedinadsDataSource) -> pd.DataFrame:
        """
        Point of entry for data retrieval in the connector

        Requires:
        - Datasource
        - Secrets
        """
        # Retrieve the access token
        access_token = self.get_access_token()
        if not access_token:
            raise NoCredentialsError('No credentials')
        headers = {'Authorization': f'Bearer {access_token}'}

        # Parse provided dates
        try:
            splitted_start = datetime.strptime(data_source.start_date, '%d/%m/%Y')
        except ValueError:
            splitted_start = dateutil.parser.parse(data_source.start_date)
        # Build the query, 1 mandatory parameters
        query = (
            f'dateRange.start.day={splitted_start.day}&dateRange.start.month={splitted_start.month}'
            f'&dateRange.start.year={splitted_start.year}&timeGranularity={data_source.time_granularity.value}'
        )

        if data_source.end_date:
            try:
                splitted_end = datetime.strptime(data_source.end_date, '%d/%m/%Y')
            except ValueError:
                splitted_end = dateutil.parser.parse(data_source.end_date)
            query += f'&dateRange.end.day={splitted_end.day}&dateRange.end.month={splitted_end.month}&dateRange.end.year={splitted_end.year}'

        # Build the query, 2 optional array parameters
        if data_source.parameters:
            for p in data_source.parameters.keys():
                query += f'&{p}={data_source.parameters.get(p)}'

        # Get the data
        res = requests.get(
            url=f'{self._baseroute}{data_source.finder_methods.value}',
            params=query,
            headers=headers,
        )

        try:
            assert res.ok
            data = res.json().get('elements')

        except AssertionError:
            raise HttpError(res.text)

        res = pd.DataFrame(data)

        if data_source.flatten_column:
            return json_to_table(res, columns=[data_source.flatten_column])
        return res

    def get_status(self) -> ConnectorStatus:
        """
        Test the Linkedin Ads connexion.

        If successful, returns a message with the email of the connected user account.
        """
        try:
            access_token = self.get_access_token()
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')

        if not access_token:
            return ConnectorStatus(status=False, error='Credentials are missing')

        return ConnectorStatus(status=True, message='Connector status OK')
