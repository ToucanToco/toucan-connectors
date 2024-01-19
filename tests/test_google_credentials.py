import json

import pytest
from pytest_mock import MockFixture

from toucan_connectors.google_credentials import GoogleCredentials, get_google_oauth2_credentials


@pytest.fixture
def google_creds_raw_conf() -> dict[str, str]:
    return {
        'type': 'service_account',
        'project_id': 'my_project_id',
        'private_key_id': 'my_private_key_id',
        'private_key': '-----BEGIN PRIVATE KEY-----\naaa\nbbb\n-----END PRIVATE KEY-----\n',
        'client_email': 'my_client_email',
        'client_id': 'my_client_id',
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
        'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/xxx.iam.gserviceaccount.com',  # noqa: E501
    }


def test_google_credentials(mocker: MockFixture, google_creds_raw_conf: dict[str, str]):
    credentials = GoogleCredentials(**google_creds_raw_conf)
    # Ensure `private_key_id` and `private_key` are masked
    assert credentials.json() == json.dumps(
        {
            **google_creds_raw_conf,
            'private_key_id': '**********',
            'private_key': '**********',
        }
    )
    # Ensure `Credentials` is called with the right values of secrets
    mock_credentials = mocker.patch('toucan_connectors.google_credentials.Credentials')
    get_google_oauth2_credentials(credentials)
    mock_credentials.from_service_account_info.assert_called_once_with(google_creds_raw_conf)


def test_unespace_break_lines(google_creds_raw_conf: dict[str, str]):
    google_creds_raw_conf[
        'private_key'
    ] = '-----BEGIN PRIVATE KEY-----\\naaa\\nbbb\\n-----END PRIVATE KEY-----\\n'
    credentials = GoogleCredentials(**google_creds_raw_conf)
    assert (
        credentials.private_key.get_secret_value() == '-----BEGIN PRIVATE KEY-----\n'
        'aaa\n'
        'bbb\n'
        '-----END PRIVATE KEY-----\n'
    )


def test_config_with_no_secrets(google_creds_raw_conf: dict[str, str]):
    google_creds_raw_conf.pop('private_key_id')
    google_creds_raw_conf.pop('private_key')
    credentials = GoogleCredentials(**google_creds_raw_conf)
    assert credentials.private_key is None
    assert credentials.private_key_id is None

    with pytest.raises(ValueError, match='key data'):
        get_google_oauth2_credentials(credentials)
