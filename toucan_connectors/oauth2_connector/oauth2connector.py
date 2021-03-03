import json
from abc import ABC, abstractmethod
from time import time
from typing import Any
from urllib import parse as url_parse

from authlib.common.security import generate_token
from authlib.integrations.requests_client import OAuth2Session
from pydantic import BaseModel, SecretStr


class SecretsKeeper(ABC):
    @abstractmethod
    def save(self, key: str, value):
        """
        Save secrets in a secrets repository
        """

    @abstractmethod
    def load(self, key: str) -> Any:
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
            self.authorization_url, state=json.dumps(state)
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
            json.loads(saved_flow['state'])['token'] == json.loads(url_params['state'][0])['token']
        )

        token = client.fetch_token(
            self.token_url,
            authorization_response=authorization_response,
            client_id=self.config.client_id,
            client_secret=self.config.client_secret.get_secret_value(),
            **kwargs,
        )
        self.secrets_keeper.save(self.auth_flow_id, token)

    def get_access_token(self) -> str:
        """
        Returns the access_token to use to access resources
        If necessary, this token will be refreshed
        """
        token = self.secrets_keeper.load(self.auth_flow_id)

        if 'expires_at' in token and token['expires_at'].timestamp() < time():
            if 'refresh_token' not in token:
                raise NoOAuth2RefreshToken
            client = OAuth2Session(
                client_id=self.config.client_id,
                client_secret=self.config.client_secret.get_secret_value(),
            )
            new_token = client.refresh_token(self.token_url, refresh_token=token['refresh_token'])
            self.secrets_keeper.save(self.auth_flow_id, new_token)
        return self.secrets_keeper.load(self.auth_flow_id)['access_token']

    def get_refresh_token(self) -> str:
        """
        Return the refresh token, used to obtain an access token
        """
        return self.secrets_keeper.load(self.auth_flow_id)['refresh_token']


class NoOAuth2RefreshToken(Exception):
    """
    Raised when no refresh token is available to get new access tokens
    """


class AuthFlowNotFound(Exception):
    """
    Raised when we could not match the given state
    """
