from enum import Enum
import json
import re
from typing import List

from pydantic import BaseModel
from requests import Session
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from requests_oauthlib import OAuth1, OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient


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


def oauth2_backend(token_url, client_id, client_secret):
    oauthclient = BackendApplicationClient(client_id=client_id)
    oauthsession = OAuth2Session(client=oauthclient)
    token = oauthsession.fetch_token(
        token_url=token_url, client_id=client_id, client_secret=client_secret)
    return OAuth2Session(client_id=client_id, token=token)


class AuthType(str, Enum):
    basic = "basic"
    digest = "digest"
    oauth1 = "oauth1"
    oauth2_backend = "oauth2_backend"


class Auth(BaseModel):
    type: AuthType
    args: List[str]

    def get_session(self) -> Session:
        auth_class = {
            'basic': HTTPBasicAuth,
            'digest': HTTPDigestAuth,
            'oauth1': OAuth1,
            'oauth2_backend': oauth2_backend
        }.get(self.type.value)

        auth_instance = auth_class(*self.args)

        # Some authentification mechanisms are built-in a Session...
        if isinstance(auth_instance, Session):
            return auth_instance

        # ... but other are just added as the auth attr of the Session
        session = Session()
        session.auth = auth_instance
        return session


def nosql_apply_parameters_to_query(query, parameters):
    """
    WARNING : DO NOT USE THIS WITH VARIANTS OF SQL
    Instead use your client library parameter substitution method.
    https://www.owasp.org/index.php/Query_Parameterization_Cheat_Sheet
    """
    if parameters is None:
        return query
    parameters = {key: json.dumps(val) for key, val in parameters.items()}
    if type(query) is str:
        query = query % parameters
    else:
        query = re.sub(r'"(%\(\w*\)s)"', r'\g<1>', json.dumps(query))
        query = json.loads(query % parameters)
    return query
