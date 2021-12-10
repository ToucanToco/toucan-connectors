import datetime
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from authlib.integrations.base_client import OAuthError
from pydantic import Field, PrivateAttr
from requests import Session

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.oauth2_connector.oauth2connector import (
    NoOAuth2RefreshToken,
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

AUTHORIZATION_URL_PROD = 'https://login.salesforce.com/services/oauth2/authorize'
AUTHORIZATION_URL_SANDBOX = 'https://test.salesforce.com/services/oauth2/authorize'

SCOPE = 'full api refresh_token'
# In Sandbox case, TOKEN_URL must be set to https://login.salesforce.com/services/oauth2/token
TOKEN_URL_PROD = 'https://login.salesforce.com/services/oauth2/token'
TOKEN_URL_SANDBOX = 'https://test.salesforce.com/services/oauth2/token'

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
        self._oauth2_connector = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL_SANDBOX
            if self.type == 'SalesforceSandbox'
            else AUTHORIZATION_URL_PROD,
            scope=SCOPE,
            token_url=TOKEN_URL_SANDBOX if self.type == 'SalesforceSandbox' else TOKEN_URL_PROD,
            secrets_keeper=kwargs['secrets_keeper'],
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
        )

    def build_authorization_url(self, **kwargs):
        return self._oauth2_connector.build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        """
        In the Salesforce's oAuth2 authentication process, client_id & client_secret
        must be sent in the body of the request so we have to set them in
        the parent class. This way they will be added to the get_access_token method
        """
        return self._oauth2_connector.retrieve_tokens(authorization_response)

    def get_access_data(self):
        return self._oauth2_connector.get_access_data()

    def _retrieve_data(self, data_source: SalesforceDataSource) -> pd.DataFrame:
        logging.getLogger(__name__).info('_retrieve_data with Salesforce Connector')
        ts_start = datetime.datetime.now().timestamp()
        access_data = self.get_access_data()
        logging.getLogger(__name__).debug(f'Retrieve connection information {access_data}')

        if not access_data:
            raise NoCredentialsError(NO_CREDENTIALS_ERROR)
        headers = {
            'Authorization': f'Bearer {access_data["access_token"]}',
            'Content-type': 'application/json',
            'Accept-Encoding': 'gzip',
        }
        session = Session()
        session.headers.update(headers)
        result = pd.DataFrame(
            self.generate_rows(
                session,
                data_source,
                instance_url=access_data['instance_url'],
                endpoint=DATA_ENDPOINT,
                params={'q': data_source.query},
            )
        )
        ts_end = datetime.datetime.now().timestamp()
        logging.getLogger(__name__).info(f'_retrieve_data finished in {ts_end - ts_start} ms')
        return result

    def generate_rows(
        self,
        session: Session,
        data_source: SalesforceDataSource,
        instance_url: str,
        endpoint: str,
        params={},
    ):
        results = self.make_request(
            session, data_source, instance_url=instance_url, data=params, endpoint=endpoint
        )

        if isinstance(results, list) and 'errorCode' in results[0]:
            logging.getLogger(__name__).error(
                f'Impossible to retrieve data with error {results[0]["errorCode"]} '
                f'and message {results[0]["message"]}'
            )
            error = f'[{results[0]["errorCode"]}] {results[0]["message"]}'
            raise SalesforceApiError(error)

        results.get('records', None)
        records = [
            {k: v for k, v in d.items() if k != 'attributes'} for d in results.get('records', None)
        ]
        logging.getLogger(__name__).debug(f'records ({len(records)}) - {str(records)}')
        next_page = results.get('nextRecordsUrl', None)
        if records:
            if next_page:
                logging.getLogger(__name__).debug('next_page exists')
                records += self.generate_rows(
                    session, data_source, instance_url=instance_url, endpoint=next_page
                )
        return records

    def make_request(
        self,
        session: Session,
        data_source: SalesforceDataSource,
        instance_url: str,
        endpoint: str,
        data={},
    ):
        logging.getLogger(__name__).info(
            f'Generate Salesforce request ' f'{instance_url}/{endpoint} with params {str(data)}'
        )
        r = session.request('GET', url=f'{instance_url}/{endpoint}', params=data).json()
        return r

    def get_status(self) -> ConnectorStatus:
        """
        Test the Salesforce's connexion.
        :return: a ConnectorStatus with the current status
        """
        try:
            access_data = self.get_access_data()
            if access_data:
                return ConnectorStatus(status=True, message='Connection successful')
            else:
                return ConnectorStatus(status=False, error='Impossible to retrieve access_token')
        except OAuthError as ex:
            return ConnectorStatus(status=False, error=f'Error to get status - {ex.error}')
        except NoOAuth2RefreshToken:
            return ConnectorStatus(
                status=False, error='Error to get status - no refresh token found'
            )
        except Exception as ex:
            return ConnectorStatus(
                status=False, error=f'Error to get status - unknown exception - {ex}'
            )
