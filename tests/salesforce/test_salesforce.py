import logging
from pathlib import Path

import pytest
import responses
from authlib.integrations.base_client import OAuthError
from requests import Session

from toucan_connectors.common import HttpError
from toucan_connectors.oauth2_connector.oauth2connector import NoOAuth2RefreshToken, OAuth2Connector
from toucan_connectors.salesforce.salesforce_connector import (
    NoCredentialsError,
    SalesforceApiError,
    SalesforceConnector,
)

import_path = "toucan_connectors.salesforce.salesforce_connector"


def test_build_authorization_url(mocker, sc):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "test_client_id"
    mock_oauth2_connector.client_secret = "test_client_secret"
    sc._oauth2_connector = mock_oauth2_connector
    sc.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_tokens(mocker, sc):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "test_client_id"
    mock_oauth2_connector.client_secret = "test_client_secret"
    sc._oauth2_connector = mock_oauth2_connector
    sc.retrieve_tokens("bla")
    mock_oauth2_connector.retrieve_tokens.assert_called()


@responses.activate
def test_make_request(sc, ds):
    """Check that the make_requests correctly calls the endpoint and return records"""
    responses.add(
        responses.GET,
        "https://salesforce.is.awsome/services/data/v39.0/query",
        json={
            "attributes": ["a", "b"],
            "records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        },
    )

    resp = sc.make_request(Session(), ds, "https://salesforce.is.awsome", "services/data/v39.0/query")
    assert resp == {
        "attributes": ["a", "b"],
        "records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
    }


def test_get_status_no_secrets(sc, remove_secrets):
    """
    Check that the connection status is false when no secret is defined
    """
    status = sc.get_status().status
    logging.getLogger(__name__).info(f"status {status}")
    assert sc.get_status().status is False


def test_get_status_secrets_error(mocker, sc):
    """
    Check that the connector status is false if the
    secret manager is not able to retrieve the access token
    """
    mocker.patch(f"{import_path}.OAuth2Connector.get_access_data", side_effect=Exception)
    assert sc.get_status().status is False


def test_get_status_secrets_auth_error(mocker, sc):
    """
    Check that the connector status is false if the
    secret manager is not able to retrieve the access token
    """
    mocker.patch(f"{import_path}.OAuth2Connector.get_access_data", side_effect=OAuthError)
    assert sc.get_status().status is False


def test_get_status_api_down(mocker, sc):
    """
    Check that the connection status is false when the secret manager receives an httperror
    """
    mocker.patch.object(SalesforceConnector, "get_access_data", side_effect=HttpError)
    assert sc.get_status().status is False


def test_get_status_ok(mocker, sc):
    """
    Check that we get the connector status set to True if
    the access token is correctly retrieved
    """
    mocker.patch.object(SalesforceConnector, "get_access_data", return_value={"access_token": "access_token"})
    assert sc.get_status().status is True


def test_get_status_nok(mocker, sc):
    mocker.patch.object(SalesforceConnector, "get_access_data", return_value=None)
    assert sc.get_status().status is False


def test_generate_rows(mocker, sc, ds, toys_results_p1, toys_results_p2):
    """Check that generate_rows handles pagination and records extraction"""
    mocked_make_request = mocker.patch.object(
        SalesforceConnector,
        "make_request",
        side_effect=[
            toys_results_p1,
            toys_results_p2,
        ],
    )
    res = sc.generate_rows(Session(), ds, "https://salesforce.is.awsome", "bla")
    assert mocked_make_request.call_count == 2
    assert res == [
        {"Id": "A111FA", "Name": "Magic Poney"},
        {"Id": "A111FB", "Name": "Wonderful Panther"},
        {"Id": "A111FC", "Name": "Lightling Lizard"},
    ]


def test_generate_rows_error(mocker, sc, ds, error_result):
    """Check that generate_rows handles errors while queryin the API"""

    mocker.patch.object(SalesforceConnector, "make_request", return_value=error_result)
    with pytest.raises(SalesforceApiError):
        sc.generate_rows(Session(), ds, "https://salesforce.is.awsome", "bla")


def test__retrieve_data_no_credentials(mocker, sc, ds, clean_p1):
    mocker.patch.object(SalesforceConnector, "get_access_data", return_value=None)
    with pytest.raises(NoCredentialsError):
        sc._retrieve_data(sc)


def test__retrieve_data(mocker, sc, ds, clean_p1):
    """Check that the connector is able to retrieve data from Salesforce API"""
    secret_object = {
        "access_token": "shiny token",
        "signature": "shiny",
        "scope": "refresh_token api full",
        "instance_url": "https://salesforce.is.awsome",
        "id": "https://login.salesforce.com/id/00D09000007vcxHEAQ/00509000006bXeyAAE",
        "token_type": "Bearer",
        "issued_at": "1621949493610",
        "refresh_token": "shiny token",
    }
    mocker.patch.object(SalesforceConnector, "get_access_data", return_value=secret_object)
    mocked_generate_rows = mocker.patch.object(SalesforceConnector, "generate_rows", return_value=clean_p1)
    res = sc._retrieve_data(ds)
    assert mocked_generate_rows.call_count == 1
    assert res.iloc[0]["Id"] == "A111FA"


def test_get_secrets_form(mocker, sc):
    """Check that the doc for oAuth setup is correctly retrieved"""
    mocker.patch("toucan_connectors.salesforce.salesforce_connector.os.path.dirname", return_value="fakepath")
    mocker.patch.object(Path, "read_text", return_value="<h1>Awesome Doc</h1>")
    doc = sc.get_connector_secrets_form()
    assert doc.documentation_md == "<h1>Awesome Doc</h1>"


def test__retrieve_data_no_secret(sc, ds, remove_secrets):
    """Checks that we have an exception as we secret was removed"""
    with pytest.raises(NoOAuth2RefreshToken):
        sc._retrieve_data(sc)


def test_can_instantiate_light_salesforce_connector() -> None:
    light_connector = SalesforceConnector(name="salesforce", auth_flow_id="auth-flow-id", instance_url="super_url")

    assert light_connector.auth_flow_id == "auth-flow-id"
    assert light_connector._oauth2_connector.secrets_keeper is None
    assert light_connector._oauth2_connector.config is None
