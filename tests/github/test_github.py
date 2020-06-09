import pytest
from toucan_connectors.github.github_connector import GithubConnector, GithubDataSource
from pydantic import ValidationError
import json
import pandas as pd


@pytest.fixture
def github_connector(bearer_api_key):
    return GithubConnector(name='test',
     bearer_auth_token=bearer_api_key)

def test_token_undefined(github_connector):
    """ It should raise an error as no token is given """
    with pytest.raises(ValidationError):
       github_connector = GithubConnector(name='test')

def test_raise_on_empty_query(github_connector):
    """It should not try to retrieve data as no query is given"""
    with pytest.raises(ValidationError):
        GithubDataSource(repo_name="toucan-connectors",
        owner="ToucanToco", domain='test', name='test')

def test_wrong_token(github_connector):
    """It should raise an error from API response as token is wrong"""
    with pytest.raises(Exception):
        github_connector = GithubConnector(name='test',
        bearer_auth_token="mytokeniswrong")
        ds = GithubDataSource(repo_name="toucan-connectors",
        owner="ToucanToco", query={'query': query},
        domain='test', name='test')
        res = github_connector.get_df(ds)

def test_malformed_query(github_connector):
    """It should raise an error from API response as query is invalid"""
    with pytest.raises(Exception):
        ds = GithubDataSource(repo_name="toucan-connectors",
        owner="ToucanToco", query={'query': "This Query Is Wrong"},
        domain='test', name='test')
        res = github_connector.get_df(ds)

def test_get_open_pr(github_connector):
    """Builds test datasource with open pull requests query"""
    query = """{
    repository(owner: "ToucanToco", name: "toucan-connectors") {
    pullRequests(last: 20, states: OPEN,
    orderBy: {field: UPDATED_AT, direction: DESC}) {
    edges {node {title updatedAt}}}}}"""

    ds = GithubDataSource(repo_name="toucan-connectors",
    owner="ToucanToco", query={'query': query},
    domain='test', name='test')
    res = github_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert len(res) > 0