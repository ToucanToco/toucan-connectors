from pydantic import BaseModel


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


def get_google_oauth2_credentials(google_credentials):
    from google.oauth2.service_account import Credentials
    return Credentials.from_service_account_info(google_credentials.dict())
