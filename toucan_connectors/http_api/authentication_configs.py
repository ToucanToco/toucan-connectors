import logging
from abc import ABC, abstractmethod
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field
from urllib import parse as url_parse

from toucan_connectors.common import UI_HIDDEN
from requests import Session

from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.oauth2_connector.oauth2connector import SecretsKeeper, oauth_client

_LOGGER = logging.getLogger(__name__)


class AuthenticationConfig(BaseModel, ABC):
    """Base class for authentication configs"""

    @abstractmethod
    def authenticate_session(self) -> Session:
        """Create authenticated request session"""

    @abstractmethod
    def is_oauth_config(self) -> bool:
        """Return true if authentication is OAuth based"""


class CustomTokenServer(AuthenticationConfig):
    kind: Literal["CustomTokenServer"] = Field(..., **UI_HIDDEN)

    def authenticate_session(self) -> Session:
        return Session()

    def is_oauth_config(self) -> bool:
        return False


class BaseOAuth2Config(AuthenticationConfig, ABC):
    """Base class for OAuth2 authentication configs"""

    # Mandatory hidden fields for oauth2 dance which must be setup by the backend and not by the end-user
    auth_flow_id: str | None = Field(None, **UI_HIDDEN)
    redirect_uri: str | None = Field(None, **UI_HIDDEN)
    secrets_keeper: SecretsKeeper | None = Field(None, **UI_HIDDEN)

    @abstractmethod
    def build_authorization_uri(self, **kwargs):
        pass

    @abstractmethod
    def retrieve_token(self, authorization_response: str, **kwargs):
        pass

    @abstractmethod
    def secrets_names(self):
        pass


class OAuth2(BaseOAuth2Config):
    kind: Literal["OAuth2"] = Field(..., **UI_HIDDEN)

    client_id: str
    client_secret: str
    authentication_url: str
    token_url: str
    scope: str

    def secrets_names(self) -> list[str]:
        return ["client_secret"]

    def authenticate_session(self) -> Session:


    def get_access_token(self):
        """
        Methods returns only access_token
        instance_url parameters are return by service, better to use it
        new method get_access_data return all information to connect (secret and instance_url)
        """
        token = self.secrets_keeper.load(self.auth_flow_id)

        if "expires_at" in token:
            expires_at = token["expires_at"]
            if isinstance(expires_at, bool):
                is_expired = expires_at
            elif isinstance(expires_at, (int, float)):
                is_expired = expires_at < time()
            else:
                is_expired = expires_at.timestamp() < time()

            if is_expired:
                if "refresh_token" not in token:
                    raise NoOAuth2RefreshToken
                client = _client(
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret.get_secret_value(),
                )
                new_token = client.refresh_token(self.token_url, refresh_token=token["refresh_token"])
                self.secrets_keeper.save(self.auth_flow_id, new_token)

        return self.secrets_keeper.load(self.auth_flow_id)["access_token"]

    def is_oauth_config(self) -> bool:
        return True

    def build_authorization_uri(self, **kwargs):
        """Build an authorization request that will be sent to the client.

        :param kwargs: Additional values that will be passed to the authorization url and retrieved in
         the 'state' query param when calling the redirect uri. That 'state' query param will be sent as a json string.
        """
        from authlib.common.security import generate_token

        client = oauth_client(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope=self.scope,
        )
        state = {"token": generate_token(), **kwargs}
        uri, state = client.create_authorization_url(self.authorization_url, state=JsonWrapper.dumps(state))

        self.secrets_keeper.save(self.auth_flow_id, {"state": state})
        return uri

    def retrieve_token(self, response_query_params: dict[str, Any]):
        client = oauth_client(
            client_id=self.config.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
        )
        saved_flow = self.secrets_keeper.load(self.auth_flow_id)
        # if saved_flow is None:
        #     raise AuthFlowNotFound()
        assert JsonWrapper.loads(saved_flow["state"])["token"] == JsonWrapper.loads(
            response_query_params["state"][0]
        )["token"]

        token = client.fetch_token(
            self.token_url,
            client_id=self.config.client_id,
            client_secret=self.client_secret.get_secret_value(),
        )
        self.secrets_keeper.save(self.auth_flow_id, token)


HttpAuthenticationConfig = (
    CustomTokenServer | OAuth2
)