from toucan_connectors.google_credentials import GoogleCredentials


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
    assert credentials.private_key == "-----BEGIN PRIVATE KEY-----\naaa\nbbb\n-----END PRIVATE KEY-----\n"
