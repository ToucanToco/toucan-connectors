import pandas
import pytest
from pydantic import ValidationError

from toucan_connectors.github.github_connector import GithubConnector, GithubDataSource


def test_no_user():
    """ It should raise an error as no token is given """
    with pytest.raises(ValidationError):
        GithubConnector()


def test_get_df():
    connector = GithubConnector(name="github", token='...')
    df = connector.get_df(GithubDataSource(name='test', domain='test',
                                           mapping=[['data', 'repositoryOwner', 'repositories', 'nodes']],
                                           query='''
    query{
        repositoryOwner(login: "toucanToco") {
            repositories(first:100){
                nodes{
                    name,
                    pullRequests(states:OPEN){
                        totalCount
                }
            }
        }
  }
}'''))
    # print(df.sort_values(['pullRequests.totalCount'], ascending=False).head(10))
    assert list(df.columns) == ['name', 'pullRequests.totalCount']
