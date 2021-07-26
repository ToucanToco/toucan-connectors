import logging
from abc import ABC, abstractmethod
from time import time
from typing import Any
from urllib import parse as url_parse

from authlib.common.security import generate_token
from authlib.integrations.requests_client import OAuth2Session
from pydantic import BaseModel, SecretStr

from toucan_connectors.json_wrapper import JsonWrapper


class SecretsKeeper(ABC):
    @abstractmethod
    def save(self, key: str, value, **kwargs):
        """
        Save secrets in a secrets repository
        """

    @abstractmethod
    def load(self, key: str, **kwargs) -> Any:
        """
        Load secrets from the secrets repository
        """


class OAuth2ConnectorConfig(BaseModel):
    client_id: str
    client_secret: SecretStr


class OAuth2Connector:
    init_params = ['secrets_keeper', 'redirect_uri'] + list(
        OAuth2ConnectorConfig.schema()['properties'].keys()
    )

    def __init__(
        self,
        auth_flow_id: str,
        authorization_url: str,
        scope: str,
        config: OAuth2ConnectorConfig,
        redirect_uri: str,
        secrets_keeper: SecretsKeeper,
        token_url: str,
    ):
        self.auth_flow_id = auth_flow_id
        self.authorization_url = authorization_url
        self.scope = scope
        self.config = config
        self.secrets_keeper = secrets_keeper
        self.token_url = token_url
        self.redirect_uri = redirect_uri

    def build_authorization_url(self, **kwargs) -> str:
        """Build an authorization request that will be sent to the client."""
        client = OAuth2Session(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret.get_secret_value(),
            redirect_uri=self.redirect_uri,
            scope=self.scope,
        )
        state = {'token': generate_token(), **kwargs}
        uri, state = client.create_authorization_url(
            self.authorization_url, state=JsonWrapper.dumps(state)
        )

        self.secrets_keeper.save(self.auth_flow_id, {'state': state})
        return uri

    def retrieve_tokens(self, authorization_response: str, **kwargs):
        url = url_parse.urlparse(authorization_response)
        url_params = url_parse.parse_qs(url.query)
        client = OAuth2Session(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret.get_secret_value(),
            redirect_uri=self.redirect_uri,
        )
        saved_flow = self.secrets_keeper.load(self.auth_flow_id)
        if saved_flow is None:
            raise AuthFlowNotFound()
        assert (
            JsonWrapper.loads(saved_flow['state'])['token']
            == JsonWrapper.loads(url_params['state'][0])['token']
        )

        token = client.fetch_token(
            self.token_url,
            authorization_response=authorization_response,
            client_id=self.config.client_id,
            client_secret=self.config.client_secret.get_secret_value(),
            **kwargs,
        )
        self.secrets_keeper.save(self.auth_flow_id, token)

    # Deprecated
    def get_access_token(self) -> str:
        """
        Methods returns only access_token
        instance_url parameters are return by service, better to use it
        new method get_access_data return all information to connect (secret and instance_url)
        """
        token = self.secrets_keeper.load(self.auth_flow_id)

        if 'expires_at' in token:

            expires_at = token['expires_at']
            if isinstance(expires_at, bool):
                is_expired = expires_at
            elif isinstance(expires_at, (int, float)):
                is_expired = expires_at < time()
            else:
                is_expired = expires_at.timestamp() < time()

            if is_expired:
                if 'refresh_token' not in token:
                    raise NoOAuth2RefreshToken
                client = OAuth2Session(
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret.get_secret_value(),
                )
                new_token = client.refresh_token(
                    self.token_url, refresh_token=token['refresh_token']
                )
                self.secrets_keeper.save(self.auth_flow_id, new_token)

        return self.secrets_keeper.load(self.auth_flow_id)['access_token']

    def get_access_data(self):
        """
        Returns the access_token to use to access resources
        If necessary, this token will be refreshed
        """
        access_data = self.secrets_keeper.load(self.auth_flow_id)

        logging.getLogger(__name__).debug('Refresh and get access data')

        if 'refresh_token' not in access_data:
            raise NoOAuth2RefreshToken
        if 'instance_url' not in access_data:
            raise NoInstanceUrl

        client = OAuth2Session(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret.get_secret_value(),
        )
        connection_data = client.refresh_token(
            self.token_url, refresh_token=access_data['refresh_token']
        )
        logging.getLogger(__name__).debug(
            f'Refresh and get access data new token {str(connection_data)}'
        )

        self.secrets_keeper.save(self.auth_flow_id, connection_data)
        secrets = self.secrets_keeper.load(self.auth_flow_id)

        logging.getLogger(__name__).debug('Refresh and get data finished')
        return secrets

    def get_refresh_token(self) -> str:
        """
        Return the refresh token, used to obtain an access token
        """
        return self.secrets_keeper.load(self.auth_flow_id)['refresh_token']


class NoOAuth2RefreshToken(Exception):
    """
    Raised when no refresh token is available to get new access tokens
    """


class NoInstanceUrl(Exception):
    """
    Raised when no instance url is available to execute request
    """


class AuthFlowNotFound(Exception):
    """
    Raised when we could not match the given state
    """
