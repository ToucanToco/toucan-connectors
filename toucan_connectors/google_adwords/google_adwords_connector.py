# https://developers.google.com/adwords/api/docs/guides/call-structure
import os
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Optional, Type

import pandas as pd
import requests
from googleads import AdWordsClient, adwords, oauth2
from pydantic import Field
from zeep.helpers import serialize_object

from toucan_connectors.common import ConnectorStatus, HttpError
from toucan_connectors.google_adwords.helpers import apply_filter, clean_columns
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

AUTHORIZATION_URL: str = (
    'https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent'
)
USER_AGENT: str = 'toucantoco.com:reportextractor:v1'
TOKEN_URL: str = 'https://oauth2.googleapis.com/token'
SCOPE: str = 'https://www.googleapis.com/auth/adwords'


class GoogleAdwordsDataSource(ToucanDataSource):
    service: str = Field(
        None,
        title='Service',
        description='Service to Query',
    )
    columns: str = Field(..., title='Columns', description='Fields to select in the dataset')
    from_clause: str = Field(
        None, title='From', description='From clause, for report extraction only'
    )
    parameters: dict = Field(
        None, title='Filter', description='A dict such as {"Column": {"Operator": "Value"}}'
    )
    during: str = Field(
        None,
        title='During',
        description='During clause, for report extraction only see: https://developers.google.com/adwords/api/docs/guides/awql#using_awql_with_reports ',
    )
    orderby: Dict = Field(
        None,
        title='Order By',
        description='Fields to sort on, e.g. {"column":"Id", "direction":"Asc"}, for service extraction only',
    )
    limit: str = Field(
        None,
        title='Limit',
        description='Max number of rows to extract, for service extraction only',
    )

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['GoogleAdwordsDataSource']) -> None:
            keys = schema['properties'].keys()
            prio_keys = [
                'service',
                'columns',
                'from_clause',
                'parameters',
                'during',
                'orderby',
                'limit',
            ]
            new_keys = prio_keys + [k for k in keys if k not in prio_keys]
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}


class GoogleAdwordsConnector(ToucanConnector):
    data_source_model: GoogleAdwordsDataSource
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]
    developer_token: str = None
    client_customer_id: str = None
    _oauth_trigger = 'instance'
    oauth2_version = Field('1', **{'ui.hidden': True})

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
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
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
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def get_refresh_token(self):
        return self.__dict__['_oauth2_connector'].get_refresh_token()

    def get_status(self) -> ConnectorStatus:
        """
        Test the Google Ads connexion.

        If successful, returns a message with the email of the connected user account.
        """
        try:
            access_token = self.get_access_token()
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')

        if not access_token:
            return ConnectorStatus(status=False, error='Credentials are missing')

        try:
            user_info = requests.get(
                url='https://www.googleapis.com/oauth2/v2/userinfo?alt=json',
                headers={'Authorization': f'Bearer {access_token}'},
            ).json()
            return ConnectorStatus(status=True, message=f"Connected as {user_info.get('email')}")
        except HttpError:
            return ConnectorStatus(status=False, error="Couldn't retrieve user infos")

    def authenticate_client(self) -> AdWordsClient:
        """Configures an oAuth Adwords client"""
        oauth2_client = oauth2.GoogleRefreshTokenClient(
            client_id=self.__dict__['_oauth2_connector'].config.client_id,
            client_secret=self.__dict__[
                '_oauth2_connector'
            ].config.client_secret.get_secret_value(),
            refresh_token=self.get_refresh_token(),
        )
        # Configure an AdWordsClient
        client = AdWordsClient(
            developer_token=self.developer_token,
            oauth2_client=oauth2_client,
            user_agent=USER_AGENT,
            client_customer_id=self.client_customer_id,
        )
        return client

    @staticmethod
    def prepare_service_query(client: AdWordsClient, data_source: GoogleAdwordsDataSource):
        """
        Prepare a query on the data_source defined service
        with the data_source given clauses
        """
        service = client.GetService(data_source.service)
        service_query_builder = adwords.ServiceQueryBuilder()
        # Build select
        service_query_builder.Select(data_source.columns)
        # Build Where
        apply_filter(service_query_builder, data_source.parameters)
        # Build Orderby
        service_query_builder.OrderBy(
            data_source.orderby['column'], data_source.orderby['direction']
        )
        # Build Limit
        if not data_source.limit:
            data_source.limit = 100
        service_query_builder.Limit(0, int(data_source.limit))

        return service, service_query_builder.Build()

    @staticmethod
    def prepare_report_query(client: AdWordsClient, data_source: GoogleAdwordsDataSource):
        """
        Prepare a query on the data_source defined report (From clause)
        with the data_source given clauses
        """
        report_downloader = client.GetReportDownloader()
        report_query_builder = adwords.ReportQueryBuilder()
        # Build select
        report_query_builder.Select(data_source.columns)
        # Build from
        report_query_builder.From(data_source.from_clause)
        # Build where
        apply_filter(report_query_builder, data_source.parameters)
        # Build during
        report_query_builder.During(data_source.during.strip())
        return report_downloader, report_query_builder.Build()

    def _retrieve_data(self, data_source: GoogleAdwordsDataSource) -> pd.DataFrame:
        """
        Point of entry for data retrieval in the connector
        This oAuth authentication is a bit different than usual.
        This flow is implemented:
        https://github.com/googleads/googleads-python-lib/wiki/API-access-on-behalf-of-your-clients-(web-flow)
        Creates & prepares the adhoc query builder and extracts the data
        """
        client = self.authenticate_client()

        if data_source.service:
            results = []
            service, query = self.prepare_service_query(client, data_source)

            for page in query.Pager(service):
                if 'entries' in page:
                    results.extend([serialize_object(e) for e in page['entries']])
            return pd.DataFrame(results)[clean_columns(data_source.columns)]
        else:
            output = StringIO()
            report_downloader, query = self.prepare_report_query(client, data_source)
            report_downloader.DownloadReportWithAwql(
                query,
                'CSV',
                output,
                skip_report_header=True,
                skip_column_header=False,
                skip_report_summary=True,
                include_zero_impressions=True,
            )
            output.seek(0)
            return pd.read_csv(output)
