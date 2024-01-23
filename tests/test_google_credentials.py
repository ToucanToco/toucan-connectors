import json

from pytest_mock import MockFixture

from toucan_connectors.google_credentials import GoogleCredentials, get_google_oauth2_credentials


def test_google_credentials(mocker: MockFixture):
    conf = {
        "type": "service_account",
        "project_id": "my_project_id",
        "private_key_id": "my_private_key_id",
        "private_key": "-----BEGIN PRIVATE KEY-----\naaa\nbbb\n-----END PRIVATE KEY-----\n",
        "client_email": "my_client_email",
        "client_id": "my_client_id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xxx.iam.gserviceaccount.com",  # noqa: E501
    }
    credentials = GoogleCredentials(**conf)
    # Ensure `private_key_id` and `private_key` are masked
    assert credentials.model_dump_json() == json.dumps(
        {
            **conf,
            "private_key_id": "**********",
            "private_key": "**********",
        },
        separators=(",", ":"),
    )
    # Ensure `Credentials` is called with the right values of secrets
    mock_credentials = mocker.patch("toucan_connectors.google_credentials.Credentials")
    get_google_oauth2_credentials(credentials)
    mock_credentials.from_service_account_info.assert_called_once_with(conf)


def test_unespace_break_lines():
    conf = {
        "type": "service_account",
        "project_id": "my_project_id",
        "private_key_id": "my_private_key_id",
        "private_key": "-----BEGIN PRIVATE KEY-----\\naaa\\nbbb\\n-----END PRIVATE KEY-----\\n",
        "client_email": "my_client_email",
        "client_id": "my_client_id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xxx.iam.gserviceaccount.com",  # noqa: E501
    }
    credentials = GoogleCredentials(**conf)
    assert (
        credentials.private_key.get_secret_value() == "-----BEGIN PRIVATE KEY-----\n"
        "aaa\n"
        "bbb\n"
        "-----END PRIVATE KEY-----\n"
    )
