import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from urllib import parse as url_parse

from dateutil.relativedelta import relativedelta
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, SecretStr, ValidationError
from requests import Session

from toucan_connectors.common import UI_HIDDEN
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.oauth2_connector.oauth2connector import oauth_client

_LOGGER = logging.getLogger(__name__)


try:
    # Module can be missing if toucan-connectors is installed in light mode
    # Those missing modules must be also included to HttpApiConnector module dependencies
    from authlib.common.security import generate_token
except ImportError:
    _LOGGER.warning("Missing dependencies for HttpApi Connector authentication")


class Oauth2Error(Exception):
    """Base class for oauth2 related errors"""


class MissingRefreshTokenError(Oauth2Error):
    """Raised when refresh token is missing from saved token"""


class MissingOauthWorkflowError(Oauth2Error):
    """Raised when the oauth2 workflow is missing from saved session."""


def validate_expires_at(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.astimezone(UTC) < datetime.now().astimezone(UTC):
        raise ValueError(f"Token expiration date {value} cannot be a past date.")
    return value


def validate_expires_in(value: int | None) -> int | None:
    if value is None:
        return None
    parsed_value = datetime.now() + relativedelta(seconds=int(value))
    if parsed_value < datetime.now():
        raise ValueError(f"Token expiration date {value} cannot be a past date.")
    return value


class TokenExpiration(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    expires_at: Annotated[datetime | None, AfterValidator(validate_expires_at)] = Field(None, alias="expiresAt")
    expires_in: Annotated[int | None, AfterValidator(validate_expires_in)] = Field(None, alias="expiresIn")

    def expires_at_timestamp(self) -> float:
        """Returns expires_at value as timestamp"""
        if self.expires_at is None:
            # For mypy, method will not be called if expires_at is None
            return datetime.now().timestamp()
        return self.expires_at.timestamp()

    def expires_in_timestamp(self) -> float:
        """Returns expires_in value as timestamp"""
        return (datetime.now() + relativedelta(seconds=(self.expires_in or 0))).timestamp()


def _extract_expiration_timestamp_from_token_response(token_response: dict[str, Any], default_lifetime: int) -> float:
    """Read token expiration info from oauth2 dance response and return computed expiration timestamp."""
    token_expiration = TokenExpiration(**token_response)
    if token_expiration.expires_at:
        return token_expiration.expires_at_timestamp()
    elif token_expiration.expires_in:
        return token_expiration.expires_in_timestamp()
    else:
        _LOGGER.warning("Can't extract token expiration dates from oauth2 response, using default.")
        return (datetime.now() + relativedelta(seconds=default_lifetime)).timestamp()


class OauthTokenSecretData(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    access_token: str = Field(..., alias="accessToken")

    # Not mandatory because oauth2 token providers can return an access_token which cannot expire.
    refresh_token: str | None = Field(None, alias="refreshToken")
    expires_at: float | None = Field(None, alias="expiresAt")  # timestamp


class OauthStateParams(BaseModel):
    workflow_token: str


class HttpOauth2SecretsKeeper(BaseModel):
    default_token_lifetime_seconds: int = 3600
    save_callback: Callable[[str, dict[str, Any], dict[str, Any] | None], None]
    delete_callback: Callable[[str, dict[str, Any] | None], None]
    load_callback: Callable[[str, dict[str, Any] | None], dict[str, Any] | None]
    context: dict[str, Any] | None = None

    def save(self, key: str, value: dict[str, Any]) -> OauthTokenSecretData:
        """Save secrets in a secrets repository"""
        try:
            secret_data = OauthTokenSecretData(**value)
        except ValidationError as exc:
            _LOGGER.error(f"Can't instantiate oauth secret data with value_keys={list(value.keys())}", exc_info=exc)
            raise

        if secret_data.refresh_token:
            secret_data.expires_at = _extract_expiration_timestamp_from_token_response(
                token_response=value, default_lifetime=self.default_token_lifetime_seconds
            )

        # remove existing secrets
        self.delete_callback(key, self.context)
        # save new secrets
        self.save_callback(key, secret_data.model_dump(), self.context)
        return secret_data

    def load(self, key: str) -> OauthTokenSecretData | None:
        """Load secrets from the secrets repository"""
        oauth_token = self.load_callback(key, self.context)
        if oauth_token:
            return OauthTokenSecretData(**oauth_token)
        return None


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

    # Mandatory fields for oauth2 dance which must be setup by the backend and not by the end-user
    auth_flow_id: str | None = Field(None, **UI_HIDDEN)
    redirect_uri: str | None = Field(None, **UI_HIDDEN)
    _secrets_keeper: HttpOauth2SecretsKeeper | None = None

    def set_secret_keeper(self, secret_keeper: HttpOauth2SecretsKeeper):
        self._secrets_keeper = secret_keeper

    @staticmethod
    def is_oauth_config() -> bool:
        return True

    @abstractmethod
    def build_authorization_uri(
        self,
        workflow_token_saver_callback: Callable[[str, str, dict[str, Any]], None],
        workflow_callback_context: dict[str, Any],
        **kwargs,
    ) -> str:
        """Build an authorization request that will be used to initialize oauth2 dance.

        :param kwargs: Additional values that will be passed to the authorization url and retrieved in
         the 'state' query param when calling the redirect uri. That 'state' query param will be sent as a json string.

        :param workflow_token_saver_callback: function used to save the generated workflow token.
        The workflow token is used in redirect callback (once the user has logged in with the external service)
        to check if it is a legit oauth workflow.

        :param workflow_callback_context: the context which will be passed to the workflow token saver callback
        """

    @abstractmethod
    def retrieve_token(
        self,
        workflow_token_loader_callback: Callable[[str, dict[str, Any]], str | None],
        workflow_callback_context: dict[str, Any],
        authorization_response: str,
    ) -> None:
        """Retrieve authorization token from oauth2 backend and save it."""


class AuthorizationCodeOauth2(BaseOAuth2Config):
    """Authorization code configuration type"""

    kind: Literal["AuthorizationCodeOauth2"] = Field(..., **UI_HIDDEN)

    # Allows to instantiate authentication config without secrets
    client_secret: SecretStr | None = None

    def authenticate_session(self) -> Session:
        session = Session()
        session.headers.update({"Authorization": f"Bearer {self._get_access_token()}"})
        return session

    def _init_oauth_client(self, **kwargs) -> Any:
        if self.client_secret is None:
            raise ValueError("Client secret field is missing to build oauth client.")
        return oauth_client(client_id=self.client_id, client_secret=self.client_secret.get_secret_value(), **kwargs)

    def build_authorization_uri(
        self,
        workflow_token_saver_callback: Callable[[str, str, dict[str, Any]], None],
        workflow_callback_context: dict[str, Any],
        **kwargs,
    ) -> str:
        if self.auth_flow_id is None:
            raise ValueError("Auth flow id field is missing to build authorization uri.")
        client = self._init_oauth_client(
            redirect_uri=self.redirect_uri,
            scope=self.scope,
        )
        oauth_state = OauthStateParams(workflow_token=generate_token())
        uri, _ = client.create_authorization_url(
            self.authentication_url,
            state=JsonWrapper.dumps({**kwargs} | oauth_state.model_dump()),
            **self.additional_auth_params,
        )
        workflow_token_saver_callback(self.auth_flow_id, oauth_state.workflow_token, workflow_callback_context)
        return uri

    def _get_access_token(self) -> str:
        """Get access token from secrets keeper.

        Call refresh token route if the access token has expired.
        Raises if oauth token is missing.
        """
        # Check that backend fields are set
        if self.auth_flow_id is None:
            raise ValueError("Auth flow id field is missing to get access_token.")
        if self._secrets_keeper is None:
            raise ValueError("Secret Keeper not initialized.")

        oauth_token = self._secrets_keeper.load(self.auth_flow_id)
        if oauth_token is None:
            raise ValueError("No oauth token found. Please refresh your oauth2 access.")
        if oauth_token.refresh_token and oauth_token.expires_at:
            # If refresh token exists, we want to verify if the access_token is still valid
            # or if it must be refreshed.
            if datetime.fromtimestamp(oauth_token.expires_at) <= (datetime.utcnow() + relativedelta(seconds=30)):
                client = self._init_oauth_client()
                new_token = client.refresh_token(self.token_url, refresh_token=oauth_token.refresh_token)
                oauth_token = self._secrets_keeper.save(
                    self.auth_flow_id,
                    # refresh call doesn't always contain the refresh_token
                    new_token | {"refresh_token": oauth_token.refresh_token},
                )
        return oauth_token.access_token

    def retrieve_token(
        self,
        workflow_token_loader_callback: Callable[[str, dict[str, Any]], str | None],
        workflow_callback_context: dict[str, Any],
        authorization_response: str,
    ) -> None:
        # Check that backend fields are set
        if self.auth_flow_id is None:
            raise ValueError("Auth flow id field is missing to retrieve oauth tokens.")
        if self._secrets_keeper is None:
            raise ValueError("Secret Keeper not initialized.")

        url = url_parse.urlparse(authorization_response)
        url_params = url_parse.parse_qs(url.query)
        client = self._init_oauth_client()
        workflow_token = workflow_token_loader_callback(self.auth_flow_id, workflow_callback_context)
        if workflow_token is None:
            raise MissingOauthWorkflowError()

        # Verify the oauth2 workflow token
        oauth_state = OauthStateParams.model_validate_json(url_params["state"][0])
        assert workflow_token == oauth_state.workflow_token, "Saved workflow token differs from received one."

        token = client.fetch_token(
            self.token_url,
            authorization_response=authorization_response,
            # Some oauth applications needs redirect_uri in fetch_token params.
            # authorization_response does not carry it natively.
            body=url_parse.urlencode({"redirect_uri": self.redirect_uri}),
        )
        self._secrets_keeper.save(self.auth_flow_id, token)


HttpAuthenticationConfig = AuthorizationCodeOauth2
