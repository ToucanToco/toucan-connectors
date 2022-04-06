import pytest
import responses

from toucan_connectors.anaplan.anaplan_connector import AnaplanConnector


@pytest.fixture()
def anaplan_auth_response() -> dict:
    return {"tokenInfo": {"tokenValue": "SomethingNotEntirelySecret"}}


@responses.activate
def test_get_status_expect_auth_ok():
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        json={"tokenInfo": {"tokenValue": "youpi"}},
        status=200,
    )
    connector = AnaplanConnector(username="JohnDoe", password="s3cr3t", name="John's connector")
    status = connector.get_status()
    assert status.status
    assert status.error is None


@responses.activate
def test_get_status_expect_auth_failed_http_40X():
    connector = AnaplanConnector(username="JohnDoe", password="s3cr3t", name="John's connector")
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        status=401,
    )

    status = connector.get_status()
    assert not status.status
    assert 'credentials' in status.error


@responses.activate
def test_get_status_expect_auth_failed_invalid_resp_body():
    connector = AnaplanConnector(username="JohnDoe", password="s3cr3t", name="John's connector")
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        status=200,
        json={"nein": "nope"},
    )

    status = connector.get_status()
    assert not status.status
    assert 'nein' in status.error
