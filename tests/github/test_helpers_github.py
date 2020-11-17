import pytest

from toucan_connectors.github.helpers import (
    KeyNotFoundException,
    build_query_pr,
    build_query_teams,
    format_pr_row,
    format_team_df,
    format_team_row,
    get_cursor,
    get_data,
    get_members,
    get_nodes,
    get_organization,
    get_page_info,
    get_pull_requests,
    get_repositories,
    get_teams,
    has_next_page,
)


def test_build_query_pr():
    """
    Check that the function build_query_pr return a properly built query string
    """
    q = build_query_pr('foorganization')
    assert 'foorganization' in q


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
    :param extracted_pr: a pull request extracted from Github's API
    """
    formatted = format_pr_row('test_repo', extracted_pr)
    assert formatted['Repo Name'] == 'test_repo'
    assert formatted['PR Name'] == 'fix(charts): blablabla'
    assert formatted['PR Creation Date'] == '2020-11-09T12:48:16Z'
    assert formatted['PR Merging Date'] == '2020-11-12T12:48:16Z'
    assert formatted['PR Additions'] == 3
    assert formatted['PR Deletions'] == 3
    assert formatted['PR Type'] == ['foo', 'fix', 'need review']
    assert formatted['Dev'] == 'okidoki'


def test_format_pr_row_no_dev(extracted_pr_no_login):
    """
    Check that the function doesn't get dev's name as it's missing
    from extraction
    :param extracted_pr: a pull request extracted from Github's API
    """
    formatted = format_pr_row('test_repo', extracted_pr_no_login)
    assert formatted['Dev'] is None


def test_format_pr_row_no_commits(extracted_pr_no_commits):
    """
    Check that the function doesn't get dev's name as no commits are
    present in extraction
    :param extracted_pr: a pull request extracted from Github's API
    """
    formatted = format_pr_row('test_repo', extracted_pr_no_commits)
    assert formatted['Dev'] is None


def test_format_team_row():
    """
    Check that a team retrieved from Github's API is correctly formatted
    :param extracted_team: a team extracted from Github's API
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
    :param extracted_teams: a list of teams extracted from Github's API
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
    assert get_organization({'data': {'organization': 'foorganization'}}) == 'foorganization'
    with pytest.raises(KeyNotFoundException):
        get_organization({'data': {'bla': 'bla'}})


def test_get_repositories():
    """
    Check that get_repositories is able to retrieve
    the repositories list from a given dict
    """
    assert get_repositories({'data': {'organization': {'repositories': 'repos'}}}) == 'repos'
    with pytest.raises(KeyNotFoundException):
        get_repositories({'data': {'organization': {'bla': 'bla'}}})


def test_get_nodes():
    """
    Check that get_nodes is able to retrieve a list of nodes from a
    given dict
    """
    assert get_nodes({'nodes': ['node']}) == ['node']


def test_get_teams():
    """
    Check that get_nodes is able to retrieve teams from a given dict
    """
    assert get_teams({'data': {'organization': {'teams': 'teams'}}}) == 'teams'
    with pytest.raises(KeyNotFoundException):
        get_teams({'data': {'organization': {'bla': 'bla'}}})


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
    Check that get_page_infois able to retrieve pagination data
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
    Check that get_page_info is able to tell if current page has a next one
    depending on the kind of paginated data
    """
    assert has_next_page({'hasNextPage': True})
    assert not (has_next_page({'hasNextPage': False}))
    with pytest.raises(KeyNotFoundException):
        has_next_page({'bla': 'bla'})


def test_get_cursor():
    """
    Check that get_cursor is able to retrieve pagination cursor
    depending on the kind of paginated data
    """
    assert get_cursor({'endCursor': 'curs'}) == 'curs'
    with pytest.raises(KeyNotFoundException):
        get_cursor({'bl': 'a'})
