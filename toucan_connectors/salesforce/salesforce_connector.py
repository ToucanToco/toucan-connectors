import os
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import Field
from requests import Session

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

AUTHORIZATION_URL = 'https://login.salesforce.com/services/oauth2/authorize'
SCOPE = 'full api refresh_token'
# In Sandbox case, TOKEN_URL must be set to https://login.salesforce.com/services/oauth2/token
TOKEN_URL = 'https://login.salesforce.com/services/oauth2/token'
NO_CREDENTIALS_ERROR = 'No credentials'
DATA_ENDPOINT = 'services/data/v39.0/query'


class SalesforceApiError(Exception):
    """Raised when the connector receives a session expired message"""


class NoCredentialsError(Exception):
    """Raised when no access token is available."""


class SalesforceDataSource(ToucanDataSource):
    query: str = Field(
        None,
        description='The SOQL query to send',
        widget='sql',
    )


class SalesforceConnector(ToucanConnector):
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]
    data_source_model: SalesforceDataSource
    instance_url: str = Field(
        None,
        title='instance url',
        description='Baseroute URL of the salesforces instance to query (without the trailing slash)',
    )

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

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        """
        In the Salesforce's oAuth2 authentication process, client_id & client_secret
        must be sent in the body of the request so we have to set them in
        the parent class. This way they will be added to the get_access_token method
        """
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def _retrieve_data(self, data_source: SalesforceDataSource) -> pd.DataFrame:
        access_token = self.get_access_token()

        if not access_token:
            raise NoCredentialsError(NO_CREDENTIALS_ERROR)
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-type': 'application/json',
            'Accept-Encoding': 'gzip',
        }
        session = Session()
        session.headers.update(headers)
        return pd.DataFrame(
            self.generate_rows(
                session, data_source, endpoint=DATA_ENDPOINT, params={'q': data_source.query}
            )
        )

    def generate_rows(
        self, session: Session, data_source: SalesforceDataSource, endpoint: str, params={}
    ):
        results = self.make_request(session, data_source, data=params, endpoint=endpoint)
        try:
            results.get('records', None)
            records = [
                {k: v for k, v in d.items() if k != 'attributes'}
                for d in results.get('records', None)
            ]
            next_page = results.get('nextRecordsUrl', None)
            if records:
                if next_page:
                    records += self.generate_rows(session, data_source, endpoint=next_page)
            return records
        except AttributeError:
            error = results[0]['errorCode']
            raise SalesforceApiError(error)

    def make_request(
        self, session: Session, data_source: SalesforceDataSource, endpoint: str, data={}
    ):
        r = session.request('GET', url=f'{self.instance_url}/{endpoint}', params=data).json()
        return r

    def get_status(self) -> ConnectorStatus:
        """
        Test the Salesforce's connexion.
        :return: a ConnectorStatus with the current status
        """
        try:
            access_token = self.get_access_token()
            if access_token:
                c = ConnectorStatus(status=True)
                return c
            else:
                return ConnectorStatus(status=False)
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')
