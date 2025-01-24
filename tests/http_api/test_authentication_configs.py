from datetime import datetime, timedelta
from typing import Any

import pytest
from freezegun import freeze_time
from pytest_mock import MockFixture

from toucan_connectors.http_api.authentication_configs import (
    AuthorizationCodeOauth2,
    HttpOauth2SecretsKeeper,
    MissingOauthWorkflowError,
    OauthTokenSecretData,
    _extract_expiration_timestamp_from_token_response,
)
from toucan_connectors.json_wrapper import JsonWrapper


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
def auth_flow_id() -> str:
    return "my_secret_key"


@pytest.fixture
def redirect_uri() -> str:
    return "http://redirect-url"


@pytest.fixture
def oauth2_authentication_config(
    client_id: str,
    client_secret: str,
    authentication_url: str,
    token_url: str,
    scope: str,
    auth_flow_id: str,
    redirect_uri: str,
) -> AuthorizationCodeOauth2:
    return AuthorizationCodeOauth2(
        kind="AuthorizationCodeOauth2",
        client_id=client_id,
        client_secret=client_secret,
        authentication_url=authentication_url,
        token_url=token_url,
        scope=scope,
        auth_flow_id=auth_flow_id,
        redirect_uri=redirect_uri,
    )


class KeyAlreadyExistsInDatabase(Exception):
    pass


_SAVED_OAUTH_CONTENT = {}
_SAVE_WORKFLOW_TOKEN = {}


def _fake_saver(key: str, value: dict[str, Any], context: dict[str, Any]) -> None:
    if key in _SAVED_OAUTH_CONTENT:
        # Should not happen! This means that old secrets have not been
        # removed from the DB
        raise KeyAlreadyExistsInDatabase(f"key={key}")
    _SAVED_OAUTH_CONTENT[key] = value


def _fake_loader(key: str, context: dict[str, Any]) -> dict[str, Any] | None:
    return _SAVED_OAUTH_CONTENT.get(key, None)


def _fake_remover(key: str, context: dict[str, Any]) -> None:
    if key in _SAVED_OAUTH_CONTENT:
        _SAVED_OAUTH_CONTENT.pop(key)


def _fake_workflow_saver(key: str, workflow_token: str, context: dict[str, Any]) -> None:
    _SAVE_WORKFLOW_TOKEN[key] = workflow_token


def _fake_workflow_loader(key: str, context: dict[str]) -> str | None:
    if key in _SAVE_WORKFLOW_TOKEN:
        return _SAVE_WORKFLOW_TOKEN[key]


@pytest.fixture
def clear_fake_secret_database() -> None:
    _SAVED_OAUTH_CONTENT.clear()
    _SAVE_WORKFLOW_TOKEN.clear()


@pytest.fixture
def secret_keeper(clear_fake_secret_database: None) -> HttpOauth2SecretsKeeper:
    return HttpOauth2SecretsKeeper(save_callback=_fake_saver, delete_callback=_fake_remover, load_callback=_fake_loader)


@pytest.mark.usefixtures("clear_fake_secret_database")
def test_authenticate_session_with_valid_access_token(
    oauth2_authentication_config: AuthorizationCodeOauth2, auth_flow_id: str, secret_keeper: HttpOauth2SecretsKeeper
) -> None:
    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)

    # Create and save fake secret data
    secrets = OauthTokenSecretData(
        access_token="my_awesome_token",
        refresh_token="my_awesome_refresh_token",
        expires_at=(datetime.now() + timedelta(0, 3600)).timestamp(),
    ).model_dump()
    secret_keeper.save(auth_flow_id, secrets)

    retrieved_session = oauth2_authentication_config.authenticate_session()
    assert retrieved_session.headers["Authorization"] == "Bearer my_awesome_token"


@pytest.mark.usefixtures("clear_fake_secret_database")
def test_authenticate_session_with_expired_access_token(
    oauth2_authentication_config: AuthorizationCodeOauth2,
    secret_keeper: HttpOauth2SecretsKeeper,
    client_id: str,
    client_secret: str,
    token_url: str,
    auth_flow_id: str,
    mocker: MockFixture,
) -> None:
    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)

    # Create and save fake secret data
    secrets = OauthTokenSecretData(
        access_token="my_awesome_token",
        refresh_token="my_awesome_refresh_token",
        expires_at=(datetime.now() - timedelta(0, 3600)).timestamp(),
    ).model_dump()

    # We are using directly the keeper callback because we can't save an expired token
    _fake_saver(auth_flow_id, secrets, context={})

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


@pytest.mark.usefixtures("clear_fake_secret_database")
def test_build_authorization_url(
    authentication_url: str,
    client_id: str,
    auth_flow_id: str,
    oauth2_authentication_config: AuthorizationCodeOauth2,
    secret_keeper: HttpOauth2SecretsKeeper,
    mocker: MockFixture,
):
    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)
    workflow_token = "generated_token_667"
    mocker.patch("toucan_connectors.http_api.authentication_configs.generate_token", return_value=workflow_token)
    assert secret_keeper.load(auth_flow_id) is None
    auth_url = oauth2_authentication_config.build_authorization_uri(
        workflow_token_saver_callback=_fake_workflow_saver,
        workflow_callback_context={},
        random="content",
        other_token="super_123",
    )
    assert auth_url == (
        f"{authentication_url}?response_type=code&client_id={client_id}"
        "&redirect_uri=http%3A%2F%2Fredirect-url"
        f"&scope=ACCESS%3A%3AREAD%2C+ACCESS%3A%3AWRITE"
        f"&state=%7B%22random%22%3A%22content%22%2C%22other_token%22%3A%22super_123%22%2C%22"
        f"workflow_token%22%3A%22{workflow_token}%22%7D"
    )
    # workflow correctly saved
    assert _fake_workflow_loader(auth_flow_id, context={}) == workflow_token


@pytest.mark.usefixtures("clear_fake_secret_database")
def test_retrieve_oauth2_token(
    client_id: str,
    client_secret: str,
    token_url: str,
    auth_flow_id: str,
    redirect_uri: str,
    oauth2_authentication_config: AuthorizationCodeOauth2,
    secret_keeper: HttpOauth2SecretsKeeper,
    mocker: MockFixture,
) -> None:
    json_str_state = JsonWrapper.dumps({"workflow_token": "workflow_token_123", "random": "laputa stuff"})
    authorization_response = (
        "http://laputa/my_small_app/connectors/http/authentication/redirect?"
        f"state={json_str_state}"
        "&code=oauth_authorization_code_444"
        "&scope=ACCESS::READ-ACCESS::WRITE"
    )

    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)

    # As this time, workflow_token is saved
    _fake_workflow_saver(auth_flow_id, "workflow_token_123", {})

    # Expects a call to refresh the expired access token
    mocked_client = mocker.MagicMock(name="mocked_client")
    mocked_client.fetch_token.return_value = {
        "access_token": "my_access_token",
        "expires_in": 3920,
        "token_type": "Bearer",
        "scope": "ACCESS::READ, ACCESS::WRITE",
        "refresh_token": "my_refresh_token",
    }

    mock = mocker.patch("toucan_connectors.http_api.authentication_configs.oauth_client", return_value=mocked_client)
    oauth2_authentication_config.retrieve_token(
        workflow_token_loader_callback=_fake_workflow_loader,
        workflow_callback_context={},
        authorization_response=authorization_response,
    )

    # Check if fetched tokens are correctly saved
    assert secret_keeper.load(auth_flow_id).access_token == "my_access_token"
    assert secret_keeper.load(auth_flow_id).refresh_token == "my_refresh_token"

    assert mock.call_count == 1
    assert mock.call_args[1] == {"client_id": client_id, "client_secret": client_secret}
    assert mocked_client.fetch_token.call_count == 1
    assert mocked_client.fetch_token.call_args[0][0] == token_url
    assert mocked_client.fetch_token.call_args[1]["authorization_response"] == authorization_response
    assert mocked_client.fetch_token.call_args[1]["body"] == "redirect_uri=http%3A%2F%2Fredirect-url"


@pytest.mark.usefixtures("clear_fake_secret_database")
def test_raise_exception_on_refresh_token_when_saved_workflow_differs(
    client_id: str,
    client_secret: str,
    token_url: str,
    auth_flow_id: str,
    redirect_uri: str,
    oauth2_authentication_config: AuthorizationCodeOauth2,
    secret_keeper: HttpOauth2SecretsKeeper,
) -> None:
    json_str_state = JsonWrapper.dumps({"workflow_token": "invalid_workflow_token", "random": "laputa stuff"})
    authorization_response = (
        "http://laputa/my_small_app/connectors/http/authentication/redirect?"
        f"state={json_str_state}"
        "&code=oauth_authorization_code_444"
        "&scope=ACCESS::READ-ACCESS::WRITE"
    )

    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)

    # As this time, workflow_token is saved
    _fake_workflow_saver(auth_flow_id, "workflow_token_123", {})

    with pytest.raises(AssertionError):
        oauth2_authentication_config.retrieve_token(
            workflow_token_loader_callback=_fake_workflow_loader,
            workflow_callback_context={},
            authorization_response=authorization_response,
        )


@pytest.mark.usefixtures("clear_fake_secret_database")
def test_raise_exception_on_refresh_token_when_secret_keeper_is_empty(
    client_id: str,
    client_secret: str,
    token_url: str,
    auth_flow_id: str,
    redirect_uri: str,
    oauth2_authentication_config: AuthorizationCodeOauth2,
    secret_keeper: HttpOauth2SecretsKeeper,
) -> None:
    json_str_state = JsonWrapper.dumps({"workflow_token": "invalid_workflow_token", "random": "laputa stuff"})
    authorization_response = (
        "http://laputa/my_small_app/connectors/http/authentication/redirect?"
        f"state={json_str_state}"
        "&code=oauth_authorization_code_444"
        "&scope=ACCESS::READ-ACCESS::WRITE"
    )

    oauth2_authentication_config.set_secret_keeper(secret_keeper=secret_keeper)

    with pytest.raises(MissingOauthWorkflowError):
        oauth2_authentication_config.retrieve_token(
            workflow_token_loader_callback=_fake_workflow_loader,
            workflow_callback_context={},
            authorization_response=authorization_response,
        )


@pytest.fixture
def expires_at_expectations() -> dict[str, Any]:
    return {
        "with_expires_in": {
            "response": {"expires_in": 2000},
            "expected_timestamp": 1737643520.0,  # 2025-01-23 14:45:20 (UTC)
            "must_raise": False,
        },
        "with_empty_response": {
            "response": {},
            # Use default token lifetime value
            "expected_timestamp": 1737645120.0,  # 2025-01-23 15:12:00 (UTC)
            "must_raise": False,
        },
        "with_expires_at": {
            "response": {"expires_at": "2025-02-25 08:06:00"},
            "expected_timestamp": 1740467160.0,  # 2025-02-25 08:06:00 (UTC)
            "must_raise": False,
        },
        "with_expires_at_timestamp": {
            "response": {"expires_at": 1740467160.0},  # 2025-02-25 08:06:00 (Paris UTC+1)
            "expected_timestamp": 1740463560.0,  # 2025-02-25 07:06:00 (UTC)
            "must_raise": False,
        },
        "with_expires_at_with_timezone": {
            "response": {"expires_at": "2025-02-25 10:06:00+02:00"},
            "expected_timestamp": 1740467160.0,  # 2025-02-25 08:06:00 (UTC)
            "must_raise": False,
        },
        "with_unsupported_format": {
            "response": {"expires_at": "2024,12,18"},
            "expected_timestamp": None,
            "must_raise": True,
        },
        "with_past_token": {
            "response": {"expires_at": "2024-12-18 00:00:00"},
            "expected_timestamp": None,
            "must_raise": True,
        },
    }


@pytest.mark.parametrize(
    "case_name",
    [
        "with_expires_in",
        "with_empty_response",
        "with_expires_at",
        "with_expires_at_timestamp",
        "with_expires_at_with_timezone",
        "with_unsupported_format",
        "with_past_token",
    ],
)
@freeze_time("2025-01-23 14:12:00")
def test_expires_at_works_as_expected(case_name: str, expires_at_expectations: dict[str, Any]) -> None:
    if expires_at_expectations[case_name]["must_raise"]:
        with pytest.raises(ValueError):
            _extract_expiration_timestamp_from_token_response(expires_at_expectations[case_name]["response"], 3600)
    else:
        result = _extract_expiration_timestamp_from_token_response(expires_at_expectations[case_name]["response"], 3600)
        assert expires_at_expectations[case_name]["expected_timestamp"] == result
