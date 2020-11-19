import pytest
from python_graphql_client import GraphqlClient

from toucan_connectors.common import HttpError
from toucan_connectors.github.github_connector import (
    GithubConnector,
    GithubDataSource,
    NoCredentialsError,
)
from toucan_connectors.github.helpers import GithubError
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector

import_path = 'toucan_connectors.github.github_connector'


@pytest.fixture
def client():
    return GraphqlClient('https://bla.bla', {})


@pytest.fixture
def gc(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'access_token'})
    return GithubConnector(
        name='test',
        auth_flow_id='test',
        client_id='test_client_id',
        client_secret='test_client_secret',
        secrets_keeper=secrets_keeper,
        redirect_uri='https://redirect.me/',
    )


@pytest.fixture
def remove_secrets(secrets_keeper, gc):
    secrets_keeper.save('test', {'access_token': None})


def build_ds(dataset: str):
    """Builds test datasource"""
    return GithubDataSource(
        name='mah_ds', domain='mah_domain', dataset=dataset, organization='foorganization'
    )


def test_datasource():
    """Tests that default dataset on datasource is 'pull requests'"""
    ds = GithubDataSource(name='mah_ds', domain='test_domain', organization='foorganization')
    assert ds.dataset == 'pull requests'


def test_get_status_no_secrets(gc, remove_secrets):
    """
    It should fail if no secrets are provided
    """
    assert gc.get_status().status is False


def test_get_status_secrets_error(mocker, gc):
    """
    It should fail if secrets can't be retrieved
    """
    mocker.patch(f'{import_path}.OAuth2Connector.get_access_token', side_effect=Exception)
    assert gc.get_status().status is False


def test_get_status_api_down(mocker, gc):
    """
    It should fail if the third-party api is down.
    """
    mocker.patch.object(GithubConnector, 'get_access_token', side_effect=HttpError)
    assert gc.get_status().status is False


def test_get_status_ok(mocker, gc):
    """
    Check that we get the connector status set to True if
    the access token is correctly retrieved
    """
    mocker.patch.object(
        GithubConnector, 'get_access_token', return_value={'access_token': 'access_token'}
    )
    assert gc.get_status().status is True


def test_error_pr_extraction(mocker, gc, error_response):
    ds = build_ds('pull requests')

    mocker.patch('python_graphql_client.GraphqlClient.execute', return_value=error_response)

    with pytest.raises(GithubError):
        gc._retrieve_data(ds)


def test_error_get_names(mocker, gc, error_response, client):
    mocker.patch('python_graphql_client.GraphqlClient.execute', return_value=error_response)

    with pytest.raises(GithubError):
        gc.get_names(client, 'foorganization', 'teams')


def test_build_authorization_url(mocker, gc):
    """
    It should proxy OAuth2Connectors methods
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    gc.__dict__['_oauth2_connector'] = mock_oauth2_connector
    gc.build_authorization_url()
    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_data_error_no_credentials(gc, remove_secrets):
    """
    Check that an error is raised while retrieving data as secrets are removed
    """
    ds = build_ds('pull requests')

    with pytest.raises(NoCredentialsError):
        gc._retrieve_data(ds)


def test_retrieve_tokens(mocker, gc):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    gc.__dict__['_oauth2_connector'] = mock_oauth2_connector
    gc.retrieve_tokens('bla')
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
    ds = build_ds('pull requests')
    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute',
        side_effect=[extracted_repositories_names, extracted_repositories_names_2],
    )
    names = gc.get_names(client=client, organization=ds.organization, dataset=ds.dataset)
    assert mocked_api_call.call_count == 2
    assert 'repo1' in names
    assert 'repo3' in names
    ds = build_ds('teams')

    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute',
        side_effect=[extracted_team_slugs, extracted_team_slugs_2],
    )
    slugs = gc.get_names(client=client, organization=ds.organization, dataset=ds.dataset)
    assert mocked_api_call.call_count == 2
    assert 'bar' in slugs
    assert 'oof' in slugs


def test_error_get_pages(mocker, gc, error_response, client, event_loop):
    """
    Check that errors in response from members extraction are
    correctly catched
    """
    mocker.patch('python_graphql_client.GraphqlClient.execute_async', return_value=error_response)

    with pytest.raises(GithubError):
        event_loop.run_until_complete(
            gc.get_pages(name='foo', organization='foorganization', dataset='teams', client=client)
        )


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
    ds = build_ds('teams')
    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute',
        side_effect=[extracted_team_slugs, extracted_team_slugs_2],
    )
    mocked_api_call_async = mocker.patch(
        'python_graphql_client.GraphqlClient.execute_async',
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
        gc._fetch_data(dataset=ds.dataset, organization=ds.organization, client=client)
    )
    assert mocked_api_call.call_count == 2
    assert mocked_api_call_async.call_count == 7
    assert len(members_dataset) == 5


def test_fetch_pull_requests_data(
    mocker,
    gc,
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
    ds = build_ds('pull requests')
    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute', return_value=extracted_repositories_names_2
    )
    mocked_api_call_async = mocker.patch(
        'python_graphql_client.GraphqlClient.execute_async',
        side_effect=[extracted_prs_1, extracted_prs_2, extracted_prs_3],
    )
    pr_dataset = event_loop.run_until_complete(
        gc._fetch_data(dataset=ds.dataset, organization=ds.organization, client=client)
    )
    assert mocked_api_call.call_count == 1
    assert mocked_api_call_async.call_count == 3
    assert len(pr_dataset) == 5


def test_get_pages(
    mocker,
    gc,
    extracted_prs_1,
    extracted_prs_2,
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
        'python_graphql_client.GraphqlClient.execute_async',
        side_effect=[extracted_prs_1, extracted_prs_2],
    )
    pr_rows = event_loop.run_until_complete(
        gc.get_pages(
            name='repo1', organization='foorganization', dataset='pull requests', client=client
        )
    )
    assert mocked_api_call.call_count == 2
    assert len(pr_rows) == 4
    assert pr_rows[1]['Repo Name'] == 'repo1'

    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute_async',
        side_effect=[extracted_team_page_1, extracted_team_page_2],
    )
    members_rows = event_loop.run_until_complete(
        gc.get_pages(name='foo', organization='foorganization', dataset='teams', client=client)
    )
    assert mocked_api_call.call_count == 2
    assert len(members_rows) == 2


def test_retrieve_members_data(
    mocker,
    gc,
    extracted_team_slugs,
    extracted_team_slugs_2,
    extracted_team_page_1,
    extracted_team_page_2,
    client,
):
    """Check that _retrieve_data is able to retrieve data from Github's API"""
    ds = build_ds('teams')
    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute',
        side_effect=[extracted_team_slugs, extracted_team_slugs_2],
    )
    mocked_api_call_async = mocker.patch(
        'python_graphql_client.GraphqlClient.execute_async',
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
    extracted_repositories_names_2,
    extracted_prs_1,
    extracted_prs_2,
    extracted_prs_3,
    client,
):
    """
    Check that _retrieve_data function is able to retrieve pull requests dataset
    """
    ds = build_ds('pull requests')
    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute', return_value=extracted_repositories_names_2
    )
    mocked_api_call_async = mocker.patch(
        'python_graphql_client.GraphqlClient.execute_async',
        side_effect=[extracted_prs_1, extracted_prs_2, extracted_prs_3],
    )
    pr_dataset = gc._retrieve_data(ds)
    assert mocked_api_call.call_count == 1
    assert mocked_api_call_async.call_count == 3
    assert len(pr_dataset) == 5
