from pydantic import BaseModel
import json
import re


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


def apply_parameters_to_query(query, parameters):
    if parameters is None:
        return query
    parameters = {key: json.dumps(val) for key, val in parameters.items()}
    if type(query) is str:
        query = query % parameters
    else:
        query = re.sub('"(%\(\w*\)s)"', '\g<1>', json.dumps(query))
        query = json.loads(query % parameters)
    return query
