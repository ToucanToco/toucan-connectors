import pytest
from python_graphql_client import GraphqlClient

from toucan_connectors.common import HttpError
from toucan_connectors.github.github_connector import (
    GithubConnector,
    GithubDataSource,
    GithubError,
    NoCredentialsError,
)
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


def test_build_team_df(extracted_teams, gc):
    """
    Check that the build_team_rows function properly build
    a list of pandas dataframe with a column of devs names
    and a column of array of team names
    """
    formatted = gc.build_team_dict(extracted_teams)
    assert formatted['foobar'] == 'faketeam'
    assert len(formatted.keys()) == 3


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


def test_extract_teams_data_one_page(extracted_teams_one_page, mocker, gc, client):
    """
    Check that the extract_teams_data correctly extract teams data from
    Github's API and return them in a list of formatted pandas DataFrames
    """
    mocked_api_call = mocker.patch.object(
        client, 'execute', return_value=extracted_teams_one_page, spec=True
    )
    team_rows = gc.extract_teams_data(client=client, organization='foorganization')
    mocked_api_call.assert_called_once()
    assert len(team_rows) == 3


def test_extract_teams_data_two_members_page(
    extracted_teams_first_member_page, extracted_teams_second_members_page, mocker, gc, client
):
    """
    Check that the extract_teams_data correctly extract teams data from
    Github's API and return them in a list of formatted pandas DataFrames
    """
    mocked_api_call = mocker.patch.object(
        client,
        'execute',
        side_effect=[
            extracted_teams_first_member_page,
            extracted_teams_second_members_page,
        ],
        spec=True,
    )
    rows = gc.extract_teams_data(client=client, organization='foorganization')
    assert mocked_api_call.call_count == 2
    assert len(rows) == 6
    assert rows[rows['Dev'] == 'foobuzz']['teams'].tolist()[0] == ['faketeam']


def test_extract_teams_data_two_teams_page(
    extracted_teams_first_team_page, extracted_teams_second_team_page, mocker, gc, client
):
    """
    Check that extraction of the members is complete if there is another page
    """
    mocked_api_call = mocker.patch.object(
        client,
        'execute',
        side_effect=[extracted_teams_first_team_page, extracted_teams_second_team_page],
    )
    teams = gc.extract_teams_data(client=client, organization='foorganization')
    assert mocked_api_call.call_count == 2
    assert len(teams) == 6
    assert teams[teams['Dev'] == 'foobuzza']['teams'].tolist()[0] == ['faketeam']


def test_build_pr_rows(gc, extracted_pr_list):
    """
    Check that the extracted pull requests data are properly formatted
    """
    pr_rows = gc.build_pr_rows(extracted_pr_list)
    assert len(pr_rows) == 2
    assert pr_rows[0]['Repo Name'] == 'tucblabla'
    assert pr_rows[1]['PR Name'] == 'fix(something): Fix something'
    assert 'barr' in pr_rows[0]['PR Type']
    assert pr_rows[1]['Dev'] == 'jeandupont'


def test_extract_pr_data_one_page(gc, mocker, extracted_pr_list, client):
    """
    Check that pull requests data are properly extracted
    """
    mocked_api_call = mocker.patch.object(client, 'execute', return_value=extracted_pr_list)
    pr_data = gc.extract_pr_data(client=client, organization='foorganization')
    mocked_api_call.assert_called_once()
    assert pr_data['Repo Name'][0] == 'tucblabla'
    assert pr_data['PR Name'][1] == 'fix(something): Fix something'


def test_extract_pr_data_two_repo_pages(
    gc, mocker, extracted_pr_first_repo, extracted_pr_list, client
):
    """
    Check that the pr data extraction properly handles pagination on repos
    """
    mocked_api_call = mocker.patch.object(
        client, 'execute', side_effect=[extracted_pr_first_repo, extracted_pr_list]
    )
    pr_data = gc.extract_pr_data(client=client, organization='foorganization')
    assert mocked_api_call.call_count == 2
    assert pr_data['Repo Name'][0] == 'lablabla'
    assert pr_data['Repo Name'][2] == 'tucblabla'
    assert len(pr_data) == 4


def test_extract_pr_data_two_pr_pages(
    gc, mocker, extracted_pr_first_prs, extracted_pr_list, client
):
    """
    Check that the pr data extraction properly
     handles pagination on pull requests
    """
    mocked_api_call = mocker.patch.object(
        client, 'execute', side_effect=[extracted_pr_first_prs, extracted_pr_list]
    )
    pr_data = gc.extract_pr_data(client=client, organization='foorganization')
    assert mocked_api_call.call_count == 2
    assert pr_data['PR Name'][0] == 'first pr'
    assert pr_data['PR Additions'][2] == 3
    assert len(pr_data) == 4


def test_retrieve_pull_requests(gc, mocker, extracted_pr_first_repo, extracted_pr_list):
    """
    Check that the build_dataset properly builds the dataset for our
    application
    """
    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute',
        side_effect=[extracted_pr_first_repo, extracted_pr_list],
    )
    ds = build_ds('pull requests')
    dataset = gc._retrieve_data(ds)
    assert mocked_api_call.call_count == 2
    assert len(dataset) == 4
    assert dataset.iloc[2]['PR Additions'] == 3


def test_retrieve_teams(
    gc, mocker, extracted_teams_first_team_page, extracted_teams_second_team_page
):
    """
    Check that the build_dataset properly builds the dataset for our
    application
    """
    mocked_api_call = mocker.patch(
        'python_graphql_client.GraphqlClient.execute',
        side_effect=[extracted_teams_first_team_page, extracted_teams_second_team_page],
    )
    ds = build_ds('teams')
    dataset = gc._retrieve_data(ds)
    assert mocked_api_call.call_count == 2
    assert len(dataset) == 6
    assert dataset.iloc[2]['Dev'] == 'barfoo'
    assert dataset.iloc[4]['teams'] == ['faketeam']


def test_error_pr_extraction(mocker, gc, error_response):
    ds = build_ds('pull requests')

    mocker.patch('python_graphql_client.GraphqlClient.execute', return_value=error_response)

    with pytest.raises(GithubError):
        gc._retrieve_data(ds)


def test_error_teams_extraction(mocker, gc, error_response):
    ds = build_ds('teams')
    mocker.patch('python_graphql_client.GraphqlClient.execute', return_value=error_response)

    with pytest.raises(GithubError):
        gc._retrieve_data(ds)


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


def test_build_pr_rows_no_pr(gc, extracted_pr_no_pr):
    """
    Check that only the record contains only the repo name
    as pull requests list is empty
    """
    pr_rows = gc.build_pr_rows(extracted_pr_no_pr)
    assert pr_rows[0]['Repo Name'] == 'empty_repo'
