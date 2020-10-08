from pytest import fixture

from toucan_connectors.github.github_connector import GithubConnector, GithubDataSource

mock_request_call = 'toucan_connectors.github.github_connector.requests.get'


@fixture
def con():
    return GithubConnector(username='username', personal_token='token', name='name')


@fixture
def ds():
    return GithubDataSource(owner='chujimmy', repo='cv', state='all')


def test_github_not_ok_should_return_empty(mocker, con, ds):
    api_mock = mocker.patch(mock_request_call)

    api_mock.return_value.ok = False

    df = con.get_df(ds)

    assert df.to_dict() == {}


def test_github_no_pull_requests_should_return_empty(mocker, con, ds):
    api_mock = mocker.patch(mock_request_call)
    api_mock.return_value.ok = True
    api_mock.return_value.json.return_value = []

    df = con.get_df(ds)

    assert df.to_dict() == {}


def test_github_should_return_pull_requests(mocker, con, ds):
    api_mock = mocker.patch(mock_request_call)
    api_mock.return_value.ok = True
    api_mock.return_value.json.return_value = [
        {
            'url': 'https://api.github.com/repos/octocat/Hello-World/pulls/1347',
            'id': 1,
            'state': 'open',
            'title': 'Amazing new feature',
        },
        {
            'url': 'https://api.github.com/repos/octocat/Hello-World/pulls/564666',
            'id': 2,
            'state': 'closed',
            'title': 'Fix bug in prod',
        },
    ]

    df = con.get_df(ds)

    api_mock.assert_called_with(
        'https://api.github.com/repos/{}/{}/pulls?state={}'.format(ds.owner, ds.repo, ds.state),
        auth=(con.username, con.personal_token),
    )
    assert df.to_dict() == {
        'url': {
            0: 'https://api.github.com/repos/octocat/Hello-World/pulls/1347',
            1: 'https://api.github.com/repos/octocat/Hello-World/pulls/564666',
        },
        'id': {0: 1, 1: 2},
        'state': {0: 'open', 1: 'closed'},
        'title': {0: 'Amazing new feature', 1: 'Fix bug in prod'},
    }


def test_github_should_not_pass_state_if_missing(mocker, con, ds):
    ds.state = None

    api_mock = mocker.patch(mock_request_call)
    api_mock.return_value.ok = True
    api_mock.return_value.json.return_value = []

    df = con.get_df(ds)

    api_mock.assert_called_with(
        'https://api.github.com/repos/{}/{}/pulls'.format(ds.owner, ds.repo),
        auth=(con.username, con.personal_token),
    )
    assert df.to_dict() == {}
