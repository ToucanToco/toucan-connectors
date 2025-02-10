import logging
from abc import ABC, abstractmethod
from time import time
from typing import TYPE_CHECKING, Any
from urllib import parse as url_parse

from pydantic import BaseModel, SecretStr

from toucan_connectors.json_wrapper import JsonWrapper

if TYPE_CHECKING:  # pragma: no cover
    from authlib.integrations.requests_client import OAuth2Session


def oauth_client(
    *, client_id: str, client_secret: str, redirect_uri: str | None = None, scope: str | None = None
) -> "OAuth2Session":
    from authlib.integrations.requests_client import OAuth2Session

    kwargs: dict[str, Any] = {"client_id": client_id, "client_secret": client_secret}
    if redirect_uri is not None:
        kwargs["redirect_uri"] = redirect_uri
    if scope is not None:
        kwargs["scope"] = scope

    return OAuth2Session(**kwargs)


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


class SecretKeeperMissingError(Exception):
    """Raised when secret_keeper is not set on oauth2 connector"""


class OAuth2ConnectorConfigMissingError(Exception):
    """Raised when config is not set on oauth2 connector"""


class OAuth2ConnectorConfig(BaseModel):
    client_id: str
    client_secret: SecretStr


class OAuth2Connector:
    init_params = ["secrets_keeper", "redirect_uri"] + list(OAuth2ConnectorConfig.schema()["properties"].keys())

    def __init__(
        self,
        auth_flow_id: str,
        authorization_url: str,
        scope: str,
        token_url: str,
        config: OAuth2ConnectorConfig | None = None,
        redirect_uri: str | None = None,
        secrets_keeper: SecretsKeeper | None = None,
    ):
        self.auth_flow_id = auth_flow_id
        self.authorization_url = authorization_url
        self.scope = scope
        self.config = config
        self.secrets_keeper = secrets_keeper
        self.token_url = token_url
        self.redirect_uri = redirect_uri

    def _secrets_keeper(self) -> SecretsKeeper:
        if self.secrets_keeper is None:
            raise SecretKeeperMissingError("Secret keeper is not set on oauth2 connector.")
        return self.secrets_keeper

    def _oauth_config(self) -> OAuth2ConnectorConfig:
        if self.config is None:
            raise OAuth2ConnectorConfigMissingError("Oauth2 Connector Config is not set on oauth2 connector.")
        return self.config

    def build_authorization_url(self, **kwargs) -> str:
        """Build an authorization request that will be sent to the client."""
        from authlib.common.security import generate_token

        client = oauth_client(
            client_id=self._oauth_config().client_id,
            client_secret=self._oauth_config().client_secret.get_secret_value(),
            redirect_uri=self.redirect_uri,
            scope=self.scope,
        )
        state = {"token": generate_token(), **kwargs}
        uri, state = client.create_authorization_url(self.authorization_url, state=JsonWrapper.dumps(state))

        self._secrets_keeper().save(self.auth_flow_id, {"state": state})
        return uri

    def retrieve_tokens(self, authorization_response: str, **kwargs):
        url = url_parse.urlparse(authorization_response)
        url_params = url_parse.parse_qs(url.query)
        client = oauth_client(
            client_id=self._oauth_config().client_id,
            client_secret=self._oauth_config().client_secret.get_secret_value(),
            redirect_uri=self.redirect_uri,
        )
        saved_flow = self._secrets_keeper().load(self.auth_flow_id)
        if saved_flow is None:
            raise AuthFlowNotFound()
        assert JsonWrapper.loads(saved_flow["state"])["token"] == JsonWrapper.loads(url_params["state"][0])["token"]

        token = client.fetch_token(
            self.token_url,
            authorization_response=authorization_response,
            client_id=self._oauth_config().client_id,
            client_secret=self._oauth_config().client_secret.get_secret_value(),
            **kwargs,
        )
        self._secrets_keeper().save(self.auth_flow_id, token)

    # Deprecated
    def get_access_token(self) -> str:
        """
        Methods returns only access_token
        instance_url parameters are return by service, better to use it
        new method get_access_data return all information to connect (secret and instance_url)
        """
        token = self._secrets_keeper().load(self.auth_flow_id)

        if "expires_at" in token:
            expires_at = token["expires_at"]
            if isinstance(expires_at, bool):
                is_expired = expires_at
            elif isinstance(expires_at, int | float):
                is_expired = expires_at < time()
            else:
                is_expired = expires_at.timestamp() < time()

            if is_expired:
                if "refresh_token" not in token:
                    raise NoOAuth2RefreshToken
                client = oauth_client(
                    client_id=self._oauth_config().client_id,
                    client_secret=self._oauth_config().client_secret.get_secret_value(),
                )
                new_token = client.refresh_token(self.token_url, refresh_token=token["refresh_token"])
                self._secrets_keeper().save(self.auth_flow_id, new_token)

        return self._secrets_keeper().load(self.auth_flow_id)["access_token"]

    def get_access_data(self):
        """
        Returns the access_token to use to access resources
        If necessary, this token will be refreshed
        """
        access_data = self._secrets_keeper().load(self.auth_flow_id)

        logging.getLogger(__name__).debug("Refresh and get access data")

        if "refresh_token" not in access_data:
            raise NoOAuth2RefreshToken
        if "instance_url" not in access_data:
            raise NoInstanceUrl

        client = oauth_client(
            client_id=self._oauth_config().client_id,
            client_secret=self._oauth_config().client_secret.get_secret_value(),
        )
        connection_data = client.refresh_token(self.token_url, refresh_token=access_data["refresh_token"])
        logging.getLogger(__name__).debug(f"Refresh and get access data new token {str(connection_data)}")

        self._secrets_keeper().save(self.auth_flow_id, connection_data)
        secrets = self._secrets_keeper().load(self.auth_flow_id)

        logging.getLogger(__name__).debug("Refresh and get data finished")
        return secrets

    def get_refresh_token(self) -> str:
        """
        Return the refresh token, used to obtain an access token
        """
        return self._secrets_keeper().load(self.auth_flow_id)["refresh_token"]


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
