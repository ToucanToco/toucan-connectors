import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Literal
from urllib import parse as url_parse

from authlib.common.security import generate_token
from pydantic import BaseModel, Field, SecretStr, ValidationError
from requests import Session

from toucan_connectors.common import UI_HIDDEN
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.oauth2_connector.oauth2connector import oauth_client

_LOGGER = logging.getLogger(__name__)


class Oauth2Error(Exception):
    """Base class for oauth2 related errors"""


class MissingRefreshTokenError(Oauth2Error):
    """Raised when refresh token is missing from saved token"""


class MissingOauthWorkflowError(Oauth2Error):
    """Raised when the oauth2 workflow is missing from saved session."""


_SUPPORTED_EXPIRATION_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"]


def _extract_expires_at_from_token_response(token_response: dict[str, Any], default_lifetime: int) -> datetime:
    """Read token expiration info from oauth2 dance response and return a computed datetime.

    It can raise an exception if the expiration date has un unexpected format.
    """
    if "expires_at" in token_response:
        if isinstance(token_response["expires_at"], str):
            expires_at = None
            for date_format in _SUPPORTED_EXPIRATION_FORMATS:
                try:
                    expires_at = datetime.strptime(token_response["expires_at"], date_format)
                    break
                except ValueError:
                    continue
            if expires_at is not None:
                return expires_at
            raise ValueError(f"Can't parse the oauth2 token expiration: {token_response['expires_at']}")
        return datetime.fromtimestamp(int(token_response["expires_at"]))
    else:
        return datetime.now() + timedelta(0, int(token_response.get("expires_in", default_lifetime)))


class OAuth2SecretData(BaseModel):
    workflow_token: str | None = None
    access_token: str
    refresh_token: str
    expires_at: float


class HttpOauth2SecretsKeeper(BaseModel):
    default_token_lifetime_seconds: int = 3600
    save_callback: Callable[[str, dict[str, Any]], None]
    delete_callback: Callable[[str], None]
    load_callback: Callable[[str], dict[str, Any] | None]

    def save(self, key: str, value: dict[str, Any]) -> None:
        """Save secrets in a secrets repository"""
        value["expires_at"] = _extract_expires_at_from_token_response(
            token_response=value, default_lifetime=self.default_token_lifetime_seconds
        ).timestamp()
        try:
            _LOGGER.info(f"DATA = {value}")
            secret_data = OAuth2SecretData(**value)
        except ValidationError as exc:
            _LOGGER.error(f"Can't instantiate oauth secret data with value_keys={list(value.keys())}", exc_info=exc)
            raise
        # remove existing secrets
        self.delete_callback(key)
        _LOGGER.info(f"SAVE KEY: {key}")
        # save new secrets
        self.save_callback(key, secret_data.model_dump())

    def load(self, key: str) -> dict[str, Any] | None:
        """Load secrets from the secrets repository"""
        return self.load_callback(key)


class AuthenticationConfig(BaseModel, ABC):
    """Base class for authentication configs"""

    @abstractmethod
    def authenticate_session(self) -> Session:
        """Create authenticated request session"""

    @staticmethod
    def is_oauth_config() -> bool:
        """Return true if authentication is OAuth based"""
        return False


class BaseOAuth2Config(AuthenticationConfig, ABC):
    """Base class for OAuth2 authentication configs"""

    authentication_url: str
    token_url: str
    scope: str
    additional_auth_params: dict = Field(
        default_factory=dict,
        title="Additional authentication params",
        description="A JSON object that represents additional arguments that must be passed as query params"
        " to the Oauth2 backend during token exchanges",
    )
    client_id: str

    @staticmethod
    def is_oauth_config() -> bool:
        return True

    @abstractmethod
    def build_authorization_uri(self, **kwargs):
        """Build an authorization request that will be used to initialize oauth2 dance.

        :param kwargs: Additional values that will be passed to the authorization url and retrieved in
         the 'state' query param when calling the redirect uri. That 'state' query param will be sent as a json string.
        """
        pass

    @abstractmethod
    def retrieve_token(self, response_params: dict[str, Any]):
        """Retrieve authorization token from oauth2 backend"""
        pass

    @abstractmethod
    def secrets_names(self) -> list[str]:
        """Return the list of the secret fields names"""
        pass

    # Mandatory hidden fields for oauth2 dance which must be setup by the backend and not by the end-user
    _auth_flow_id: str | None = None
    _redirect_uri: str | None = None
    _secrets_keeper: HttpOauth2SecretsKeeper | None = None

    def set_secret_keeper(self, secret_keeper: HttpOauth2SecretsKeeper):
        self._secrets_keeper = secret_keeper

    def set_redirect_uri(self, redirect_uri: str):
        self._redirect_uri = redirect_uri

    def set_auth_flow_id(self, auth_flow_id: str):
        self._auth_flow_id = auth_flow_id


class AuthorizationCodeOauth2(BaseOAuth2Config):
    """Authorization code configuration type"""

    kind: Literal["AuthorizationCodeOauth2"] = Field(..., **UI_HIDDEN)

    # Allows to instantiate authentication config without secrets
    client_secret: SecretStr | None = None

    def secrets_names(self) -> list[str]:
        return ["client_secret"]

    def authenticate_session(self) -> Session:
        session = Session()
        session.headers.update({"Authorization": f"Bearer {self._get_access_token()}"})
        return session

    def build_authorization_uri(self, **kwargs) -> str:
        client = oauth_client(
            client_id=self.client_id,
            client_secret=self.client_secret.get_secret_value(),
            redirect_uri=self._redirect_uri,
            scope=self.scope,
        )
        workflow_token = generate_token()
        state = {"workflow_token": workflow_token, **kwargs}
        uri, state = client.create_authorization_url(
            self.authentication_url, state=JsonWrapper.dumps(state), **self.additional_auth_params
        )

        tmp_oauth_secrets = {
            "workflow_token": workflow_token,
            "access_token": "__UNKNOWN__",
            "refresh_token": "__UNKNOWN__",
        }
        self._secrets_keeper.save(self._auth_flow_id, tmp_oauth_secrets)
        return uri

    def _get_access_token(self) -> str:
        """Get access token from secrets keeper.

        Call refresh token route if the access token has expired.
        """
        oauth_token_info = self._secrets_keeper.load(self._auth_flow_id)
        if "expires_at" in oauth_token_info:
            expires_at = oauth_token_info["expires_at"]
            if datetime.fromtimestamp(expires_at) < datetime.now():
                client = oauth_client(
                    client_id=self.client_id,
                    client_secret=self.client_secret.get_secret_value(),
                )
                new_token = client.refresh_token(self.token_url, refresh_token=oauth_token_info["refresh_token"])
                self._secrets_keeper.save(
                    self._auth_flow_id,
                    # refresh call doesn't always contain the refresh_token
                    new_token | {"refresh_token": oauth_token_info["refresh_token"]},
                )
        return self._secrets_keeper.load(self._auth_flow_id)["access_token"]

    def retrieve_token(self, authorization_response: str) -> None:
        url = url_parse.urlparse(authorization_response)
        url_params = url_parse.parse_qs(url.query)
        client = oauth_client(
            client_id=self.client_id,
            client_secret=self.client_secret.get_secret_value(),
        )
        saved_flow = self._secrets_keeper.load(self._auth_flow_id)
        if saved_flow is None:
            raise MissingOauthWorkflowError()

        # Verify the oauth2 workflow token
        assert saved_flow["workflow_token"] == JsonWrapper.loads(url_params["state"][0])["workflow_token"]

        token = client.fetch_token(
            self.token_url,
            authorization_response=authorization_response,
            # Some oauth applications needs redirect_uri in fetch_token params.
            # authorization_response does not carry it natively.
            body=url_parse.urlencode({"redirect_uri": self._redirect_uri}),
        )
        self._secrets_keeper.save(self._auth_flow_id, token)


HttpAuthenticationConfig = AuthorizationCodeOauth2
