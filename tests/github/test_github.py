import json
import pytest
from pydantic import ValidationError

from toucan_connectors.github.github_connector import (
    GithubConnector,
    GithubDataSource,
    Auth
)


def test_missing_authentication():
    with pytest.raises(ValidationError):
        GithubConnector()


def test_missing_query():
    with pytest.raises(ValidationError):
        GithubDataSource()


def test_get_df(mocker, data_source, connector):
    mock_requests = mocker.patch('toucan_connectors.github.github_connector.requests')
    connector.get_df(data_source)
    mock_requests.post.assert_called_once_with(
        url=data_source.github_graphql_api_url,
        auth=connector.auth,
        data=json.dumps(data_source.query)
    )


def test_cannot_json_api_result(mocker, data_source, connector):
    mock_requests = mocker.patch('toucan_connectors.github.github_connector.requests')
    mock_requests.post.return_value.json.side_effect = ValueError
    with pytest.raises(ValueError):
        connector.get_df(data_source)


@pytest.fixture(scope='function')
def data_source():
    query = {
        'query': 'query { viewer { login issues { totalCount } pullRequests { totalCount }}}'
    }
    return GithubDataSource(name='test', domain='test', query=query)


@pytest.fixture(scope='function')
def connector():
    fake_auth = Auth(type='basic', args=['username', 'password'])
    return GithubConnector(name='test', auth=fake_auth)
