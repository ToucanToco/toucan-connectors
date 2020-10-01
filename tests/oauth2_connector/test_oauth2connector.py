from typing import Any
from unittest.mock import Mock

import pytest

from toucan_connectors.oauth2_connector.oauth2connector import (
    NoOAuth2RefreshToken,
    OAuth2Connector,
    SecretsKeeper,
)

FAKE_AUTHORIZATION_URL = 'http://localhost:4242/foobar'
FAKE_TOKEN_URL = 'http://service/token_endpoint'
SCOPE: str = 'openid email https://www.googleapis.com/auth/spreadsheets.readonly'


@pytest.fixture
def secrets_keeper():
    class SimpleSecretsKeeper(SecretsKeeper):
        def __init__(self):
            self.store = {}

        def load(self, key: str) -> Any:
            return self.store[key]

        def save(self, key: str, value: Any):
            self.store[key] = value

    return SimpleSecretsKeeper()


@pytest.fixture
def oauth2_connector(secrets_keeper):
    return OAuth2Connector(
        name='test',
        authorization_url=FAKE_AUTHORIZATION_URL,
        scope=SCOPE,
        client_id='',
        client_secret='',
        redirect_uri='',
        token_url=FAKE_TOKEN_URL,
        secrets_keeper=secrets_keeper,
    )


def test_build_authorization_url(mocker, oauth2_connector, secrets_keeper):
    """
    It should return the authorization URL
    """
    mock_create_authorization_url: Mock = mocker.patch(
        'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Session.create_authorization_url',
        return_value=('authorization_url', 'state'),
    )
    url = oauth2_connector.build_authorization_url()
    mock_create_authorization_url.assert_called_once_with(FAKE_AUTHORIZATION_URL)
    assert url == 'authorization_url'
    assert secrets_keeper.load('test')['state'] == 'state'


def test_retrieve_tokens(mocker, oauth2_connector, secrets_keeper):
    """
    It should retrieve tokens and save them
    """
    secrets_keeper.save('test', {'state': 'dummy_state'})
    mock_fetch_token: Mock = mocker.patch(
        'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Session.fetch_token',
        return_value={'access_token': 'dummy_token'},
    )

    oauth2_connector.retrieve_tokens('http://localhost/?state=dummy_state')
    mock_fetch_token.assert_called()
    assert secrets_keeper.load('test')['access_token'] == 'dummy_token'


def test_fail_retrieve_tokens(oauth2_connector, secrets_keeper):
    """
    It should fail ig the stored state does not match the received state
    """
    secrets_keeper.save('test', {'state': 'dummy_state'})

    with pytest.raises(AssertionError):
        oauth2_connector.retrieve_tokens('http://localhost/?state=bad_state')


def test_get_access_token(oauth2_connector, secrets_keeper):
    """
    It should return the last saved access_token
    """
    secrets_keeper.save('test', {'access_token': 'dummy_token'})
    assert oauth2_connector.get_access_token() == 'dummy_token'


def test_get_access_token_expired(mocker, oauth2_connector, secrets_keeper):
    """
    It should refresh the token if it expired
    """
    secrets_keeper.save(
        'test',
        {'access_token': 'dummy_token', 'expires_at': 0, 'refresh_token': 'dummy_refresh_token'},
    )

    mock_refresh_token: Mock = mocker.patch(
        'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Session.refresh_token',
        return_value={'access_token': 'new_token'},
    )
    access_token = oauth2_connector.get_access_token()
    mock_refresh_token.assert_called_once_with(FAKE_TOKEN_URL, refresh_token='dummy_refresh_token')
    assert access_token == 'new_token'


def test_get_access_token_expired_no_refresh_token(mocker, oauth2_connector, secrets_keeper):
    """
    It should fail to refresh the token if no refresh token is provided
    """
    secrets_keeper.save('test', {'access_token': 'dummy_token', 'expires_at': 0})

    mock_refresh_token: Mock = mocker.patch(
        'toucan_connectors.oauth2_connector.oauth2connector.OAuth2Session.refresh_token',
        return_value={'access_token': 'new_token'},
    )
    with pytest.raises(NoOAuth2RefreshToken):
        oauth2_connector.get_access_token()
    mock_refresh_token.assert_not_called()
