from datetime import datetime

import pytest
import responses
from python_graphql_client import GraphqlClient

from toucan_connectors.common import HttpError
from toucan_connectors.github.github_connector import (
    GithubConnector,
    GithubDataSource,
    NoCredentialsError,
)
from toucan_connectors.github.helpers import GithubError
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector

import_path = "toucan_connectors.github.github_connector"


@pytest.fixture
def client():
    return GraphqlClient("https://bla.bla", {})


@pytest.fixture
def gc(secrets_keeper):
    secrets_keeper.save("test", {"access_token": "access_token"})
    return GithubConnector(
        name="test",
        auth_flow_id="test",
        client_id="test_client_id",
        client_secret="test_client_secret",
        secrets_keeper=secrets_keeper,
        redirect_uri="https://redirect.me/",
    )


@pytest.fixture
def remove_secrets(secrets_keeper, gc):
    secrets_keeper.save("test", {"access_token": None})


def build_ds(dataset: str):
    """Builds test datasource"""
    return GithubDataSource(name="mah_ds", domain="mah_domain", dataset=dataset, organization="foorganization")


def test_datasource():
    """Tests that default dataset on datasource is teams'"""
    ds = GithubDataSource(name="mah_ds", domain="test_domain", organization="foorganization")
    assert ds.dataset == "teams"


def test_get_status_no_secrets(gc, remove_secrets):
    """
    Check that the connection status is false when no secret is defined
    """
    assert gc.get_status().status is False


def test_get_status_secrets_error(mocker, gc):
    """
    Check that the connector status is false if the
    secret manager is not able to retrieve the access token
    """
    mocker.patch(f"{import_path}.OAuth2Connector.get_access_token", side_effect=Exception)
    assert gc.get_status().status is False


def test_get_status_api_down(mocker, gc):
    """
    Check that the connection status is false when the secret manager receives an httperror
    """
    mocker.patch.object(GithubConnector, "get_access_token", side_effect=HttpError)
    assert gc.get_status().status is False


def test_get_status_ok(mocker, gc):
    """
    Check that we get the connector status set to True if
    the access token is correctly retrieved
    """
    mocker.patch.object(GithubConnector, "get_access_token", return_value={"access_token": "access_token"})
    assert gc.get_status().status is True


def test_error_pr_extraction(
    mocker,
    gc,
    error_response,
    mock_extraction_start_date,
    extracted_repositories_names,
    extracted_prs_2,
):
    """
    Check that previously retrieved data is returned if a page is corrupt
    """
    ds = build_ds("pull requests")
    mocker.patch(
        "python_graphql_client.GraphqlClient.execute",
        side_effect=[extracted_repositories_names, error_response],
    )
    mocker.patch("python_graphql_client.GraphqlClient.execute_async", return_value=extracted_prs_2)
    assert len(gc._retrieve_data(ds)) > 0


def test_error_get_names(mocker, gc, extracted_team_slugs, client, error_response):
    """
    Check that previously retrieved data is returned if a page is corrupt
    """
    mocker.patch(
        "python_graphql_client.GraphqlClient.execute",
        side_effect=[extracted_team_slugs, error_response],
    )
    result = gc.get_names(client, "foorganization", "teams")
    assert len(result) > 0


def test_build_authorization_url(mocker, gc):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "test_client_id"
    mock_oauth2_connector.client_secret = "test_client_secret"
    gc._oauth2_connector = mock_oauth2_connector
    gc.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_data_error_no_credentials(gc, remove_secrets):
    """
    Check that an error is raised while retrieving data as secrets are removed
    """
    ds = build_ds("pull requests")

    with pytest.raises(NoCredentialsError):
        gc._retrieve_data(ds)


def test_retrieve_tokens(mocker, gc):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = "test_client_id"
    mock_oauth2_connector.client_secret = "test_client_secret"
    gc._oauth2_connector = mock_oauth2_connector
    gc.retrieve_tokens("bla")
    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_get_oauth_doc(gc):
    """
    Check that the oauth configuration doc is retrieved
    """
    form = GithubConnector.get_connector_secrets_form()
    assert len(form.documentation_md) > 0


def test_get_names(
    mocker,
    gc,
    extracted_repositories_names,
    extracted_repositories_names_2,
    extracted_team_slugs,
    extracted_team_slugs_2,
    client,
):
    """
    Check that get_names is able to retrieve repositories & teams names from Github's API
    """
    ds = build_ds("pull requests")
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute",
        side_effect=[extracted_repositories_names, extracted_repositories_names_2],
    )
    names = gc.get_names(client=client, organization=ds.organization, dataset=ds.dataset)
    assert mocked_api_call.call_count == 2
    assert "repo1" in names
    assert "repo3" in names
    ds = build_ds("teams")

    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute",
        side_effect=[extracted_team_slugs, extracted_team_slugs_2],
    )
    slugs = gc.get_names(client=client, organization=ds.organization, dataset=ds.dataset)
    assert mocked_api_call.call_count == 2
    assert "bar" in slugs
    assert "oof" in slugs


@pytest.mark.skip("event_loop fixture is deprecated")
def test_error_get_pages(mocker, gc, error_response, client, event_loop):
    """
    Check that errors in response from members extraction are
    correctly catched
    """
    mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[
            error_response,
            error_response,
            error_response,
            error_response,
            error_response,
        ],
    )
    mocker.patch("toucan_connectors.github.github_connector.asyncio.sleep", return_value=None)

    with pytest.raises(GithubError):
        event_loop.run_until_complete(
            gc.get_pages(
                name="foo",
                organization="foorganization",
                dataset="teams",
                client=client,
                page_limit=4,
            )
        )


@pytest.mark.skip("event_loop fixture is deprecated")
def test_corrupt_get_pages(mocker, gc, client, event_loop, extracted_team_page_1):
    """
    Check that previously retrieved data is returned if a page is corrupt
    """
    mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_team_page_1, {"corrupt": "data"}],
    )
    result = event_loop.run_until_complete(
        gc.get_pages(name="foo", organization="foorganization", dataset="teams", client=client, page_limit=2)
    )
    assert result == [{"bar": "foo", "foo": "foo", "ofo": "foo"}]


@pytest.mark.skip("event_loop fixture is deprecated")
def test_fetch_members_data(
    mocker,
    gc,
    extracted_team_slugs,
    extracted_team_slugs_2,
    extracted_team_page_1,
    extracted_team_page_2,
    client,
    event_loop,
):
    """
    Check that _retrieve_data function is able to retrieve members dataset
    """
    ds = build_ds("teams")
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute",
        side_effect=[extracted_team_slugs, extracted_team_slugs_2],
    )
    mocked_api_call_async = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[
            extracted_team_page_1,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
        ],
    )
    members_dataset = event_loop.run_until_complete(
        gc._fetch_data(dataset=ds.dataset, organization=ds.organization, client=client, page_limit=10)
    )
    assert mocked_api_call.call_count == 2
    assert mocked_api_call_async.call_count == 7
    assert len(members_dataset) == 5


@pytest.mark.skip("event_loop fixture is deprecated")
def test_fetch_pull_requests_data(
    mocker,
    gc,
    mock_extraction_start_date,
    extracted_repositories_names_2,
    extracted_prs_1,
    extracted_prs_2,
    extracted_prs_3,
    client,
    event_loop,
):
    """
    Check that _retrieve_data function is able to retrieve pull requests dataset
    """
    ds = build_ds("pull requests")
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute", return_value=extracted_repositories_names_2
    )
    mocked_api_call_async = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_1, extracted_prs_2, extracted_prs_3],
    )
    pr_dataset = event_loop.run_until_complete(
        gc._fetch_data(dataset=ds.dataset, organization=ds.organization, page_limit=10, client=client)
    )
    assert mocked_api_call.call_count == 1
    assert mocked_api_call_async.call_count == 3
    assert len(pr_dataset) == 5


@pytest.mark.skip("event_loop fixture is deprecated")
def test_get_pages(
    mocker,
    gc,
    mock_extraction_start_date,
    extracted_prs_1,
    extracted_prs_2,
    extracted_prs_3,
    extracted_team_page_1,
    extracted_team_page_2,
    client,
    event_loop,
):
    """
    Check that get_pages is able to retrieve pull requests or members data
    from Github's API
    """
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_1, extracted_prs_2],
    )
    pr_rows = event_loop.run_until_complete(
        gc.get_pages(
            name="repo1",
            organization="foorganization",
            dataset="pull requests",
            client=client,
            page_limit=2,
        )
    )
    assert mocked_api_call.call_count == 2
    assert len(pr_rows) == 4
    assert pr_rows[1]["Repo Name"] == "repo1"

    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_1, extracted_prs_2],
    )
    pr_rows = event_loop.run_until_complete(
        gc.get_pages(
            name="repo1",
            organization="foorganization",
            dataset="pull requests",
            client=client,
            page_limit=1000,
            latest_retrieved_object="feat(blalbla):bla",
        )
    )
    assert mocked_api_call.call_count == 2
    assert len(pr_rows) == 3
    assert pr_rows[-1]["PR Name"] == "chore(something): somethin"

    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_team_page_1, extracted_team_page_2],
    )
    members_rows = event_loop.run_until_complete(
        gc.get_pages(name="foo", organization="foorganization", dataset="teams", client=client, page_limit=2)
    )
    assert mocked_api_call.call_count == 2
    assert len(members_rows) == 2

    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_1, extracted_prs_3],
    )
    pr_rows = event_loop.run_until_complete(
        gc.get_pages(
            name="repo1",
            organization="foorganization",
            dataset="pull requests",
            client=client,
            page_limit=1000,
        )
    )
    assert mocked_api_call.call_count == 2
    assert len(pr_rows) == 4


def test_retrieve_members_data(
    mocker,
    gc,
    extracted_team_slugs,
    extracted_team_slugs_2,
    extracted_team_page_1,
    extracted_team_page_2,
):
    """Check that _retrieve_data is able to retrieve data from Github's API"""
    ds = build_ds("teams")
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute",
        side_effect=[extracted_team_slugs, extracted_team_slugs_2],
    )
    mocked_api_call_async = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[
            extracted_team_page_1,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
            extracted_team_page_2,
        ],
    )
    members_dataset = gc._retrieve_data(ds)
    assert mocked_api_call.call_count == 2
    assert mocked_api_call_async.call_count == 7
    assert len(members_dataset) == 5


def test_retrieve_pull_requests_data(
    mocker,
    gc,
    mock_extraction_start_date,
    extracted_repositories_names_2,
    extracted_prs_1,
    extracted_prs_2,
    extracted_prs_3,
):
    """
    Check that _retrieve_data function is able to retrieve pull requests dataset
    """
    ds = build_ds("pull requests")
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute", return_value=extracted_repositories_names_2
    )
    mocked_api_call_async = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_1, extracted_prs_2, extracted_prs_3],
    )
    pr_dataset = gc._retrieve_data(ds)
    assert mocked_api_call.call_count == 1
    assert mocked_api_call_async.call_count == 3
    assert len(pr_dataset) == 5


@responses.activate
def test_datasource_get_form_with_secret(gc):
    """
    check that organizations are retrieved with a request
    """
    ds = GithubDataSource(name="mah_ds", domain="mah_domain", dataset="teams")
    responses.add(
        responses.GET,
        "https://api.github.com/user/orgs",
        json=[{"login": "power_rangers"}, {"login": "teletubbies"}],
        status=200,
    )
    ds.get_form(connector=gc, current_config={})
    assert len(responses.calls) == 1


@responses.activate
def test_datasource_get_form_no_secret(gc, remove_secrets):
    """
    check that no organizations are retrieved
    """
    ds = GithubDataSource(name="mah_ds", domain="mah_domain", dataset="teams")
    responses.add(
        responses.GET,
        "https://api.github.com/user/orgs",
        json=[{"login": "power_rangers"}, {"login": "teletubbies"}],
        status=200,
    )
    res = ds.get_form(connector=gc, current_config={})
    assert "organization" not in res["$defs"].keys()


def test_get_slice(
    gc,
    mocker,
    mock_extraction_start_date,
    extracted_repositories_names_2,
    extracted_prs_1,
    extracted_prs_2,
):
    """Check that get_slice returns only three pages of data"""
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute", return_value=extracted_repositories_names_2
    )
    mocked_api_call_async = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_1, extracted_prs_2, extracted_prs_2, extracted_prs_2],
    )
    ds = build_ds("pull requests")
    dataslice = gc.get_slice(ds)
    assert mocked_api_call.call_count == 1
    assert mocked_api_call_async.call_count == 3
    assert len(dataslice.df) == 5


def test_get_slice_limit(
    gc,
    mocker,
    mock_extraction_start_date,
    extracted_repositories_names_2,
    extracted_prs_1,
    extracted_prs_2,
):
    """Check that get_slice returns only three pages of data"""
    mocked_api_call = mocker.patch(
        "python_graphql_client.GraphqlClient.execute", return_value=extracted_repositories_names_2
    )
    mocked_api_call_async = mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_1, extracted_prs_2, extracted_prs_2, extracted_prs_2],
    )
    ds = build_ds("pull requests")
    dataslice = gc.get_slice(ds, limit=2)
    assert mocked_api_call.call_count == 1
    assert mocked_api_call_async.call_count == 3
    assert len(dataslice.df) == 2


@responses.activate
def test_get_organizations(gc):
    """Check that get_organizations is able to retrieve a list of organization"""
    responses.add(
        responses.GET,
        "https://api.github.com/user/orgs",
        json=[{"login": "power_rangers"}, {"login": "teletubbies"}],
        status=200,
    )
    res = gc.get_organizations()
    assert len(responses.calls) == 1
    assert res == ["power_rangers", "teletubbies"]


@pytest.mark.skip("event_loop fixture is deprecated")
def test_get_rate_limit_exhausted(
    gc, mocker, mock_extraction_start_date, extracted_prs_4, extracted_prs_3, event_loop, client
):
    """Check that the connector is paused when rate limit is exhausted"""
    mockedsleep = mocker.patch("toucan_connectors.github.github_connector.asyncio.sleep")
    mockeddatetime = mocker.patch("toucan_connectors.github.helpers.datetime")
    mocked_strptime = mockeddatetime.strptime
    mocked_strptime.return_value = datetime(2021, 2, 23, 13, 26, 47)
    mocked_utcnow = mockeddatetime.utcnow
    mocked_utcnow.return_value = datetime(2021, 2, 23, 13, 26, 0)
    mocked_seconds = mockeddatetime.seconds
    mocked_seconds.return_value = 47
    mocker.patch(
        "python_graphql_client.GraphqlClient.execute_async",
        side_effect=[extracted_prs_4, extracted_prs_3],
    )
    event_loop.run_until_complete(
        gc.get_pages(
            name="repo1",
            organization="foorganization",
            dataset="pull requests",
            client=client,
            page_limit=1,
        )
    )
    assert mockedsleep.call_count == 3
    assert mockedsleep.call_args_list[1][0][0] == 48


def test_instantiate_github_connector_without_oauth_parameters() -> None:
    github = GithubConnector(name="github", auth_flow_id="uuid-1234")
    assert github.auth_flow_id == "uuid-1234"
