"""
Module containing commonly-used resources for connectors using the connector oauth manager
"""


class NoCredentialsError(Exception):
    """Raised when no secrets avaiable."""


def retrieve_secrets_from_kwargs(auth_flow_id: str, **kwargs):
    """Retrieve secrets from kwargs or throw an error if no secrets."""
    try:
        secrets = kwargs.get('secrets')(auth_flow_id=auth_flow_id)
        access_token = secrets['access_token']
    except Exception:
        raise NoCredentialsError('No credentials')

    return access_token
