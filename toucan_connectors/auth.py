from datetime import datetime
from enum import Enum
from logging import getLogger

from pydantic import BaseModel, Field

try:
    import jwt
    import requests
    from oauthlib.oauth2 import BackendApplicationClient
    from requests import Session
    from requests.auth import AuthBase, HTTPBasicAuth, HTTPDigestAuth
    from requests_oauthlib import OAuth1, OAuth2Session

    class CustomTokenServer(AuthBase):
        """
        Get a token from a request to a custom token server.
        """

        def __init__(
            self,
            method,
            url,
            params=None,
            data=None,
            headers=None,
            auth=None,
            json=None,
            filter=".",
            token_header_name: str = "Authorization",  # noqa: S107
        ):
            self.request_kwargs = {
                "method": method,
                "url": url,
                "params": params,
                "data": data,
                "headers": headers,
                "json": json,
            }
            self.auth = auth
            self.filter = filter
            self.token_header_name = token_header_name

        def __call__(self, r):
            import jq

            if self.auth:
                session = Auth(**self.auth).get_session()
            else:
                session = Session()

            res = session.request(**self.request_kwargs)
            token = jq.first(self.filter, res.json())

            # If a single string is returned by the filter default
            # on OAuth "Bearer" auth-scheme.
            if len(f"{token}".split(maxsplit=2)) == 1:
                token = f"Bearer {token}"

            r.headers[self.token_header_name] = token
            return r

except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")


def oauth2_backend(token_url, client_id, client_secret):
    oauthclient = BackendApplicationClient(client_id=client_id)
    oauthsession = OAuth2Session(client=oauthclient)
    token = oauthsession.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret)
    return OAuth2Session(client_id=client_id, token=token)


def oauth2_oidc(*args, **kwargs) -> "Session":
    """
    Get a valid access token with the provided refresh_token

    Required kwargs:
                id_token: <initial token that may need to be refreshed>,
                refresh_token: <initial refresh_token>,
                client_id: <oauth client id>,
                client_secret: <oauth client secret>,
                token_endpoint: <oauth api token endpoint>,
    """
    id_token = kwargs["id_token"]
    #  check that the id_token is not expired
    decoded = jwt.decode(kwargs["id_token"], options={"verify_signature": False})
    if datetime.fromtimestamp(decoded["exp"]) < datetime.now():
        response = requests.post(
            kwargs["token_endpoint"],
            data={
                "grant_type": "refresh_token",
                "client_id": kwargs["client_id"],
                "client_secret": kwargs["client_secret"],
                "refresh_token": kwargs["refresh_token"],
            },
        )
        id_token = response.json()["id_token"]  # we don't store it as of now

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {id_token}"})
    return session


class AuthType(str, Enum):
    basic = "basic"
    digest = "digest"
    oauth1 = "oauth1"
    oauth2_backend = "oauth2_backend"
    oauth2_oidc = "oauth2_oidc"
    custom_token_server = "custom_token_server"


# mypy complains about the `kwargs` field
class Auth(BaseModel):  # type:ignore[no-redef]
    type: AuthType = Field(  # type:ignore[call-overload]
        ...,
        description="As we rely on the python request library, we suggest that you "
        "refer to the dedicated "
        '<a href="https://2.python-requests.org/en/master/user/authentication/">documentation</a> '
        "for more details.",
        description_mimetype="text/html",
    )
    args: list[str] = Field(
        default_factory=list,
        title="Positional arguments",
        description="For example for a basic authentication, you can provide your username and password here",
    )
    kwargs: dict = Field(
        default_factory=dict,
        title="Named arguments",
        description="A JSON object with argument name as key and corresponding value as value",
    )

    def get_session(self) -> "Session":
        auth_class = {
            "basic": HTTPBasicAuth,
            "digest": HTTPDigestAuth,
            "oauth1": OAuth1,
            "oauth2_backend": oauth2_backend,
            "oauth2_oidc": oauth2_oidc,
            "custom_token_server": CustomTokenServer,
        }[self.type.value]
        auth_instance = auth_class(*self.args, **self.kwargs)

        # Some authentification mechanisms are built-in a Session...
        if isinstance(auth_instance, Session):
            return auth_instance

        # ... but other are just added as the auth attr of the Session
        session = Session()
        session.auth = auth_instance
        return session
