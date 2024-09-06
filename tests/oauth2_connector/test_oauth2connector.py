from datetime import datetime
from unittest.mock import Mock

import pytest

from toucan_connectors.http_api.http_api_connector import HttpAPIConnector
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.oauth2_connector.oauth2connector import (
    AuthFlowNotFound,
    NoInstanceUrl,
    NoOAuth2RefreshToken,
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector import SnowflakeoAuth2Connector
from toucan_connectors.toucan_connector import get_oauth2_configuration

FAKE_AUTHORIZATION_URL = "http://localhost:4242/foobar"
FAKE_TOKEN_URL = "http://service/token_endpoint"
SCOPE: str = "openid email https://www.googleapis.com/auth/spreadsheets.readonly"


@pytest.fixture
def oauth2_connector(secrets_keeper):
    return OAuth2Connector(
        auth_flow_id="test",
        authorization_url=FAKE_AUTHORIZATION_URL,
        scope=SCOPE,
        config=OAuth2ConnectorConfig(client_id="", client_secret=""),
        redirect_uri="",
        token_url=FAKE_TOKEN_URL,
        secrets_keeper=secrets_keeper,
    )


def test_build_authorization_url(mocker, oauth2_connector, secrets_keeper):
    """
    It should return the authorization URL
    """
    mock_create_authorization_url: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.create_authorization_url",
        return_value=("authorization_url", "state"),
    )

    url = oauth2_connector.build_authorization_url()

    assert mock_create_authorization_url.called
    assert url == "authorization_url"
    assert secrets_keeper.load("test")["state"] == "state"


def test_retrieve_tokens(mocker, oauth2_connector, secrets_keeper):
    """
    It should retrieve tokens and save them
    """
    secrets_keeper.save("test", {"state": JsonWrapper.dumps({"token": "the_token"})})
    mock_fetch_token: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.fetch_token",
        return_value={"access_token": "dummy_token"},
    )

    oauth2_connector.retrieve_tokens(f'http://localhost/?state={JsonWrapper.dumps({"token": "the_token"})}')
    mock_fetch_token.assert_called()
    assert secrets_keeper.load("test")["access_token"] == "dummy_token"


def test_fail_retrieve_tokens(oauth2_connector, secrets_keeper):
    """
    It should fail ig the stored state does not match the received state
    """
    secrets_keeper.save("test", {"state": JsonWrapper.dumps({"token": "the_token"})})

    with pytest.raises(AssertionError):
        oauth2_connector.retrieve_tokens(f'http://localhost/?state={JsonWrapper.dumps({"token": "bad_token"})}')


def test_get_access_token(oauth2_connector, secrets_keeper):
    """
    It should return the last saved access_token
    """
    secrets_keeper.save("test", {"access_token": "dummy_token"})
    assert oauth2_connector.get_access_token() == "dummy_token"


def test_get_access_token_expired(mocker, oauth2_connector, secrets_keeper):
    """
    It should refresh the token if it expired
    """
    secrets_keeper.save(
        "test",
        {
            "access_token": "dummy_token",
            "expires_at": datetime.fromtimestamp(0),
            "refresh_token": "dummy_refresh_token",
        },
    )

    mock_refresh_token: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.refresh_token",
        return_value={"access_token": "new_token"},
    )
    access_token = oauth2_connector.get_access_token()
    mock_refresh_token.assert_called_once_with(FAKE_TOKEN_URL, refresh_token="dummy_refresh_token")
    assert access_token == "new_token"


def test_get_access_token_expired_int_type(mocker, oauth2_connector, secrets_keeper):
    """
    It should refresh the token if it expired
    """
    secrets_keeper.save(
        "test",
        {
            "access_token": "dummy_token",
            "expires_at": 123,
            "refresh_token": "dummy_refresh_token",
        },
    )

    mock_refresh_token: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.refresh_token",
        return_value={"access_token": "new_token"},
    )
    access_token = oauth2_connector.get_access_token()
    mock_refresh_token.assert_called_once_with(FAKE_TOKEN_URL, refresh_token="dummy_refresh_token")
    assert access_token == "new_token"


def test_get_access_token_expired_bool_type(mocker, oauth2_connector, secrets_keeper):
    """
    It should refresh the token if it expired
    """
    secrets_keeper.save(
        "test",
        {
            "access_token": "dummy_token",
            "expires_at": True,
            "refresh_token": "dummy_refresh_token",
        },
    )

    mock_refresh_token: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.refresh_token",
        return_value={"access_token": "new_token"},
    )
    access_token = oauth2_connector.get_access_token()
    mock_refresh_token.assert_called_once_with(FAKE_TOKEN_URL, refresh_token="dummy_refresh_token")
    assert access_token == "new_token"


def test_get_access_token_expired_no_refresh_token(mocker, oauth2_connector, secrets_keeper):
    """
    It should fail to refresh the token if no refresh token is provided
    """
    secrets_keeper.save("test", {"access_token": "dummy_token", "expires_at": datetime.fromtimestamp(0)})

    mock_refresh_token: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.refresh_token",
        return_value={"access_token": "new_token"},
    )
    with pytest.raises(NoOAuth2RefreshToken):
        oauth2_connector.get_access_token()
    mock_refresh_token.assert_not_called()


def test_get_access_data(mocker, oauth2_connector, secrets_keeper):
    mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.refresh_token",
        return_value={
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "instance_url": "new_instance_url",
        },
    )

    secrets_keeper.save(
        "test",
        {
            "access_token": "old_token",
            "refresh_token": "old_refresh_token",
            "instance_url": "old_instance_url",
        },
    )
    access_data = oauth2_connector.get_access_data()
    assert access_data["access_token"] == "new_access_token"
    assert access_data["refresh_token"] == "new_refresh_token"
    assert access_data["instance_url"] == "new_instance_url"


def test_get_access_data_without_refresh(mocker, oauth2_connector, secrets_keeper):
    mock_refresh_token: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.refresh_token",
        return_value={
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "instance_url": "new_instance_url",
        },
    )

    secrets_keeper.save("test", {"access_token": "old_token"})
    with pytest.raises(NoOAuth2RefreshToken):
        oauth2_connector.get_access_data()
    mock_refresh_token.assert_not_called()


def test_get_access_data_without_instance(mocker, oauth2_connector, secrets_keeper):
    mock_refresh_token: Mock = mocker.patch(
        "authlib.integrations.requests_client.OAuth2Session.refresh_token",
        return_value={"access_token": "new_access_token", "refresh_token": "new_refresh_token"},
    )

    secrets_keeper.save("test", {"access_token": "old_token", "refresh_token": "old_refresh_token"})
    with pytest.raises(NoInstanceUrl):
        oauth2_connector.get_access_data()
    mock_refresh_token.assert_not_called()


def test_should_throw_if_authflow_id_not_found(oauth2_connector, secrets_keeper):
    with pytest.raises(AuthFlowNotFound):
        oauth2_connector.retrieve_tokens(f'http://localhost/?state={JsonWrapper.dumps({"token": "bad_token"})}')


def test_should_return_if_is_instance_oauth2_connector(oauth2_connector):
    assert get_oauth2_configuration(HttpAPIConnector) == (False, None)
    assert get_oauth2_configuration(SnowflakeoAuth2Connector) == (True, "connector")


def test_get_refresh_token(mocker, oauth2_connector):
    mocked_keeper = mocker.patch.object(
        oauth2_connector,
        "secrets_keeper",
    )
    mocked_load = mocked_keeper.load
    mocked_load.return_value = {"refresh_token": "bla"}
    token = oauth2_connector.get_refresh_token()
    assert token == "bla"
