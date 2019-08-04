from pydantic import BaseModel, validator


class GoogleCredentials(BaseModel):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str

    @validator('private_key')
    def unescape_break_lines(cls, v):
        """
        `private_key` is a long string like
        '-----BEGIN PRIVATE KEY-----\nxxx...zzz\n-----END PRIVATE KEY-----\n
        As the breaking line are often escaped by the client,
        we need to be sure it's unescaped
        """
        return v.replace('\\n', '\n')


def get_google_oauth2_credentials(google_credentials):
    from google.oauth2.service_account import Credentials
    return Credentials.from_service_account_info(google_credentials.dict())
