from os.path import dirname, join

import pytest

from toucan_connectors.utils.pem import sanitize_spaces_pem


@pytest.fixture
def pem_key_with_spaces() -> str:
    with open(join(dirname(__file__), 'fixtures', 'pem_key_with_spaces.pem')) as f:
        return f.read()


@pytest.fixture
def sanitized_pem_key() -> str:
    with open(join(dirname(__file__), 'fixtures', 'sanitized_pem_key.pem')) as f:
        return f.read()


@pytest.fixture
def pem_bundle_with_spaces() -> str:
    with open(join(dirname(__file__), 'fixtures', 'pem_bundle_with_spaces.pem')) as f:
        return f.read()


@pytest.fixture
def sanitized_pem_bundle() -> str:
    with open(join(dirname(__file__), 'fixtures', 'sanitized_pem_bundle.pem')) as f:
        return f.read()


def test_sanitize_spaces_pem(pem_key_with_spaces: str, sanitized_pem_key: str):
    assert sanitize_spaces_pem(pem_key_with_spaces) == sanitized_pem_key


def test_sanitize_spaces_pem_on_valid_data(sanitized_pem_key: str):
    assert sanitize_spaces_pem(sanitized_pem_key) == sanitized_pem_key


def test_sanitize_spaces_pem_bundle(pem_bundle_with_spaces: str, sanitized_pem_bundle: str):
    assert sanitize_spaces_pem(pem_bundle_with_spaces) == sanitized_pem_bundle
