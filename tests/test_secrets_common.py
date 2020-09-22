"""Module containing common functions between secrets-using connectors."""
from functools import partial

import pytest

from toucan_connectors.secrets_common import NoCredentialsError, retrieve_secrets_from_kwargs


def test_retrieve_secrets_from_kwargs_success(mocker):
    """It should return an access token."""

    def fake_fetch_secrets(small_app_id, connector_type, auth_flow_id):
        return {'access_token': 'foobar'}

    fake_kwargs = {'secrets': partial(fake_fetch_secrets, 'laputa', 'Aircall2')}

    access_token = retrieve_secrets_from_kwargs('abcd', **fake_kwargs)

    assert access_token == 'foobar'


def test_retrieve_secrets_from_kwargs_problem():
    """It should throw an error if a problem in kwargs."""
    # No kwargs at all
    with pytest.raises(NoCredentialsError) as err:
        retrieve_secrets_from_kwargs('abcd')

    assert str(err.value) == 'No credentials'

    # Empty kwargs
    empty_kwargs = {}
    with pytest.raises(NoCredentialsError):
        retrieve_secrets_from_kwargs('abcd', **empty_kwargs)

    # A secrets of null in kwargs
    secretless_kwargs = {'secrets': None}

    with pytest.raises(NoCredentialsError):
        retrieve_secrets_from_kwargs('abcd', **secretless_kwargs)
