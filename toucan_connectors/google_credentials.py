from google.oauth2.service_account import Credentials
from pydantic import BaseModel, Field, HttpUrl, field_validator

CREDENTIALS_INFO_MESSAGE = (
    "This information is provided in your "
    '<a href="https://gspread.readthedocs.io/en/latest/oauth2.html">authentication file</a> downloadable '
    'from your <a href="https://console.developers.google.com/apis/credentials">Google Console</a>'
)


class JWTCredentials(BaseModel):
    """
    For Google Credentials inside the JWT

    """

    project_id: str = Field(..., title="Project ID", description=CREDENTIALS_INFO_MESSAGE)
    jwt_token: str = Field(
        ...,
        title="JSON web token (JWT) signed",
        description="JWT signed with your service_account credentials," "see the docs of the connector for that.",
    )


class GoogleCredentials(BaseModel):
    type: str = Field("service_account", title="Service account", description=CREDENTIALS_INFO_MESSAGE)
    project_id: str = Field(..., title="Project ID", description=CREDENTIALS_INFO_MESSAGE)
    private_key_id: str = Field(..., title="Private Key ID", description=CREDENTIALS_INFO_MESSAGE)
    private_key: str = Field(
        ...,
        title="Private Key",
        description=f"A private key in the form "
        f'"-----BEGIN PRIVATE KEY-----\\nXXX...XXX\\n-----END PRIVATE KEY-----\\n". {CREDENTIALS_INFO_MESSAGE}',
    )
    client_email: str = Field(..., title="Client email", description=CREDENTIALS_INFO_MESSAGE)
    client_id: str = Field(..., title="Client ID", description=CREDENTIALS_INFO_MESSAGE)
    auth_uri: HttpUrl = Field(
        "https://accounts.google.com/o/oauth2/auth",
        title="Authentication URI",
        description=CREDENTIALS_INFO_MESSAGE,
    )
    token_uri: HttpUrl = Field(
        "https://oauth2.googleapis.com/token",
        title="Token URI",
        description=f"{CREDENTIALS_INFO_MESSAGE}. You should not need to change the default value.",
    )
    auth_provider_x509_cert_url: HttpUrl = Field(
        "https://www.googleapis.com/oauth2/v1/certs",
        title="Authentication provider X509 certificate URL",
        description=f"{CREDENTIALS_INFO_MESSAGE}. You should not need to change the default value.",
    )
    client_x509_cert_url: HttpUrl = Field(
        "https://www.client_cert.test",
        title="Client X509 certification URL",
        description=CREDENTIALS_INFO_MESSAGE,
    )

    @field_validator("private_key")
    @classmethod
    def unescape_break_lines(cls, v):
        """
        `private_key` is a long string like
        '-----BEGIN PRIVATE KEY-----\nxxx...zzz\n-----END PRIVATE KEY-----\n
        As the breaking line are often escaped by the client,
        we need to be sure it's unescaped
        """
        return v.replace("\\n", "\n")


def get_google_oauth2_credentials(google_credentials: GoogleCredentials) -> Credentials:
    return Credentials.from_service_account_info(google_credentials.dict())
