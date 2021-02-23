import pytest

from toucan_connectors.github.helpers import (
    GithubError,
    KeyNotFoundException,
    RateLimitExhaustedException,
    build_query_members,
    build_query_pr,
    build_query_repositories,
    build_query_teams,
    format_pr_row,
    format_pr_rows,
    format_team_df,
    format_team_row,
    get_cursor,
    get_data,
    get_edges,
    get_errors,
    get_members,
    get_message,
    get_nodes,
    get_organization,
    get_page_info,
    get_pull_requests,
    get_rate_limit_info,
    get_repositories,
    get_repository,
    get_team,
    get_teams,
    has_next_page,
)


def test_build_query_members():
    """
    Check that the function build_query_members return a properly built query string
    """
    q = build_query_members('foorganization', 'footeam')
    assert 'foorganization' in q
    assert 'footeam' in q


def test_build_query_repositories():
    """
    Check that the function build_query_repositories return a properly built query string
    """
    q = build_query_repositories('foorganization')
    assert 'foorganization' in q


def test_build_query_pr():
    """
    Check that the function build_query_pr return a properly built query string
    """
    q = build_query_pr('foorganization', 'foorepo')
    assert 'foorganization' in q
    assert 'foorepo' in q


def test_build_query_teams():
    """
    Check that the function build_query_teams return a properly built query string
    """
    q = build_query_teams('foorganization')
    assert 'foorganization' in q


def test_format_pr_row(extracted_pr):
    """
    Check that the formatting of pull requests data extracted from Github are
    correctly formatted
    """
    formatted = format_pr_row(extracted_pr)
    assert formatted['PR Name'] == 'fix(charts): blablabla'
    assert formatted['PR Creation Date'] == '2020-11-09T12:48:16Z'
    assert formatted['PR Merging Date'] == '2020-11-12T12:48:16Z'
    assert formatted['PR Additions'] == 3
    assert formatted['PR Deletions'] == 3
    assert formatted['PR Type'] == ['foo', 'fix', 'need review']
    assert formatted['Dev'] == 'okidoki'


def test_format_pr_row_no_commits(extracted_pr_no_commits):
    """
    Check that the function doesn't get dev's name as no commits are
    present in extraction
    """
    formatted = format_pr_row(extracted_pr_no_commits)
    assert formatted['Dev'] is None


def test_format_pr_rows(extracted_pr_list):
    """
    Check that format_pr_rows is able to format a list of prs
    """
    formatted = format_pr_rows(extracted_pr_list, 'buzz')
    assert len(formatted) == 2  # CLOSED PR should be filtered out
    assert formatted[1]['Repo Name'] == 'buzz'


def test_format_team_row():
    """
    Check that a team retrieved from Github's API is correctly formatted
    """
    formatted = format_team_row(
        {'edges': [{'node': {'login': 'bla'}}, {'node': {'login': 'ba'}}]}, 'faketeam'
    )
    assert len(formatted) == 2
    assert formatted['bla'] == 'faketeam'


def test_format_team_df(team_rows):
    """
    Check that the list of team retrieved from Github's
    API is correctly built as a DataFrame
    """
    formatted = format_team_df(team_rows)
    assert len(formatted) == 5
    assert formatted[formatted['Dev'] == 'foobuzz']['teams'].values[0] == ['faketeam']
    assert 'faketeam2' in (formatted[formatted['Dev'] == 'barfoo']['teams'].values[0])
    assert 'faketeam' in (formatted[formatted['Dev'] == 'barfoo']['teams'].values[0])


def test_get_data():
    """
    Check that get_data is able to retrieve the
    data content from Github's API response
    """
    assert get_data({'data': 'some data'}) == 'some data'
    with pytest.raises(KeyNotFoundException):
        get_data({'bla': 'bla'})


def test_get_organization():
    """
    Check that get_organization is able to retrieve the
    organization content from a given dict
    """
    assert get_organization({'organization': 'foorganization'}) == 'foorganization'
    with pytest.raises(KeyNotFoundException):
        get_organization({'bla': 'bla'})


def test_get_repositories():
    """
    Check that get_repositories is able to retrieve
    the repositories list from a given dict
    """
    assert get_repositories({'repositories': 'repos'}) == 'repos'
    with pytest.raises(KeyNotFoundException):
        get_repositories({'bla': 'bla'})


def test_get_repository():
    """
    Check that get_repository is able to retrieve the repository from dict
    """
    assert get_repository({'repository': 'repo1'})
    with pytest.raises(KeyNotFoundException):
        get_repository({'repo': 'repo1'})


def test_get_nodes():
    """
    Check that get_nodes is able to retrieve a list of nodes from a
    given dict
    """
    assert get_nodes({'nodes': ['node']}) == ['node']


def test_get_team():
    """
    Check that get_team is able to retrieve team from a given dict
    """
    assert get_team({'team': 'team'}) == 'team'
    with pytest.raises(KeyNotFoundException):
        get_team({'bla': 'bla'})


def test_get_teams():
    """
    Check that get_teams is able to retrieve teams from a given dict
    """
    assert get_teams({'teams': 'teams'}) == 'teams'
    with pytest.raises(KeyNotFoundException):
        get_teams({'bla': 'bla'})


def test_get_pull_requests():
    """
    Check that get_pull requests is able to retrieve a list of pull requests from a
    given dict
    """
    assert get_pull_requests({'pullRequests': 'PR'}) == 'PR'

    with pytest.raises(KeyNotFoundException):
        get_pull_requests({'bla': 'bla'})


def test_get_page_info():
    """
    Check that get_page_info able to retrieve pagination data
    """
    assert get_page_info({'pageInfo': {'foo': 'bar'}}) == {'foo': 'bar'}
    with pytest.raises(KeyNotFoundException):
        get_page_info({'infoPage': {'bar': 'foo'}})


def test_get_members():
    """
    Check that get_members is able to retrieve members data from a Github's response
    """
    assert get_members({'members': 'members'}) == 'members'

    with pytest.raises(KeyNotFoundException):
        get_members({'data': {'organization': {'teams': {'nodes': [{'bla': 'bla'}]}}}})


def test_has_next_page():
    """
    Check that has_next_page is able to retrieve hasNextPage from page_info
    """
    assert has_next_page({'hasNextPage': True})
    assert not (has_next_page({'hasNextPage': False}))
    with pytest.raises(KeyNotFoundException):
        has_next_page({'bla': 'bla'})


def test_get_cursor():
    """
    Check that get_cursor is able to retrieve pagination cursor
    """
    assert get_cursor({'endCursor': 'curs'}) == 'curs'
    with pytest.raises(KeyNotFoundException):
        get_cursor({'bl': 'a'})


def test_get_edges():
    """
    Check that get_edges is able to retrieve a list of edges
    extracted from Github's Data
    """
    assert get_edges({'edges': ['edges']}) == ['edges']
    with pytest.raises(KeyNotFoundException):
        get_edges({'adges': ['adges']})


def test_get_errors():
    """
    Check that get_errors is able to retrieve an error message
    from Github's API response
    """
    with pytest.raises(GithubError):
        assert (
            get_errors({'errors': ['this is an error message', 'and another error']})
            == 'this is an error message'
        )


def test_get_message():
    """
    Check that get_message is able to extra a message from Github's
    API response
    """
    with pytest.raises(GithubError):
        assert get_message({'documentation_url': 'bla', 'message': 'bla'}) == 'bla'


def test_get_rate_limit_info():
    """
    Check that get_rate_limit_info is able to extract
    rate limit info from the API response
    """
    with pytest.raises(RateLimitExhaustedException):
        get_rate_limit_info({'rateLimit': {'remaining': 0, 'resetAt': '2021-02-23T13:26:47Z'}})
