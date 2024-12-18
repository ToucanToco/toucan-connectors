from datetime import datetime, timedelta
from typing import Any

import pytest
from pytest_mock import MockFixture

from toucan_connectors.http_api.authentication_configs import HttpOauth2SecretsKeeper, OAuth2Config, OAuth2SecretData


@pytest.fixture
def client_id() -> str:
    return "client_id_xxx_123"


@pytest.fixture
def client_secret() -> str:
    return "client_secret_yyy_345"


@pytest.fixture
def authentication_url() -> str:
    return "https//oauth2.backend.server/authorize"


@pytest.fixture
def token_url() -> str:
    return "https//oauth2.backend.server/fetch/token"


@pytest.fixture
def scope() -> str:
    return "ACCESS::READ, ACCESS::WRITE"


@pytest.fixture
def oauth2_authentication_config(
    client_id: str, client_secret: str, authentication_url: str, token_url: str, scope: str
) -> OAuth2Config:
    return OAuth2Config(
        kind="OAuth2Config",
        client_id=client_id,
        client_secret=client_secret,
        authentication_url=authentication_url,
        token_url=token_url,
        scope=scope,
    )


class KeyAlreadyExistsInDatabase(Exception):
    pass


_SAVED_CONTENT = {}


def _fake_saver(key: str, value: dict[str, Any]) -> None:
    if key in _SAVED_CONTENT:
        raise KeyAlreadyExistsInDatabase(f"key={key}")
    _SAVED_CONTENT[key] = value


def _fake_loader(key: str) -> dict[str, Any] | None:
    return _SAVED_CONTENT.get(key, None)


def _fake_remover(key: str) -> None:
    if key in _SAVED_CONTENT:
        _SAVED_CONTENT.pop(key)


@pytest.fixture
def secret_keeper() -> HttpOauth2SecretsKeeper:
    return HttpOauth2SecretsKeeper(save_callback=_fake_saver, delete_callback=_fake_remover, load_callback=_fake_loader)


def test_secret_names(oauth2_authentication_config: OAuth2Config) -> None:
    assert oauth2_authentication_config.secrets_names() == ["client_secret"]


def test_authenticate_session_with_valid_access_token(
    oauth2_authentication_config: OAuth2Config, secret_keeper: HttpOauth2SecretsKeeper
) -> None:
    auth_flow_id = "my_secret_key"
    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)
    oauth2_authentication_config.set_auth_flow_id(auth_flow_id=auth_flow_id)

    # Create and save fake secret data
    secrets = OAuth2SecretData(
        access_token="my_awesome_token",
        refresh_token="my_awesome_refresh_token",
        expires_at=(datetime.now() + timedelta(0, 3600)).timestamp(),
    ).model_dump()
    secret_keeper.save(auth_flow_id, secrets)

    retrieved_session = oauth2_authentication_config.authenticate_session()
    assert retrieved_session.headers["Authorization"] == "Bearer my_awesome_token"


def test_authenticate_session_with_expired_access_token(
    oauth2_authentication_config: OAuth2Config,
    secret_keeper: HttpOauth2SecretsKeeper,
    client_id: str,
    client_secret: str,
    token_url: str,
    mocker: MockFixture,
) -> None:
    auth_flow_id = "my_secret_key"
    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)
    oauth2_authentication_config.set_auth_flow_id(auth_flow_id=auth_flow_id)

    # Create and save fake secret data
    secrets = OAuth2SecretData(
        access_token="my_awesome_token",
        refresh_token="my_awesome_refresh_token",
        expires_at=(datetime.now() - timedelta(0, 3600)).timestamp(),
    ).model_dump()
    secret_keeper.save(auth_flow_id, secrets)

    # Expects a call to refresh the expired access token
    mocked_client = mocker.MagicMock(name="mocked_client")
    mocked_client.refresh_token.return_value = {
        "access_token": "new_access_token",
        "expires_in": 3920,
        "scope": "ACCESS::READ, ACCESS::WRITE",
        "token_type": "Bearer",
    }
    mock = mocker.patch("toucan_connectors.http_api.authentication_configs.oauth_client", return_value=mocked_client)
    retrieved_session = oauth2_authentication_config.authenticate_session()
    assert mock.call_count == 1
    assert mock.call_args[1] == {"client_id": client_id, "client_secret": client_secret}
    assert mocked_client.refresh_token.call_count == 1
    assert mocked_client.refresh_token.call_args[0][0] == token_url
    assert mocked_client.refresh_token.call_args[1]["refresh_token"] == "my_awesome_refresh_token"

    assert retrieved_session.headers["Authorization"] == "Bearer new_access_token"
