from toucan_connectors.github.helpers import (
    build_query_pr,
    build_query_teams,
    format_pr_row,
    format_team_df,
    format_team_row,
    get_members_list_cursor,
    get_pr_cursor,
    get_repo_cursor,
    get_team_list_cursor,
    members_list_has_next_page,
    pr_list_has_next_page,
    repo_list_has_next_page,
    team_list_has_next_page,
)


def test_build_query_pr():
    """
    Check that the function build_query_pr return a properly built query string
    """
    q = build_query_pr('foorganization')
    assert 'foorganization' in q


def test_build_query_teams():
    """
    Check that the function build_query_pr return a properly built query string
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


def test_format_team_row(extracted_team):
    """
    Check that a team retrieved from Github's API is correctly formatted
    :param extracted_team: a team extracted from Github's API
    """
    formatted = format_team_row(extracted_team)
    assert len(formatted) == 3
    assert formatted['foobuzz'] == 'faketeam'


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


def test_repo_list_has_next_page(extracted_pr_first_repo):
    """
    Check that the repo_list_has_next_page function properly
    detects a next page in repositories list
    """
    assert repo_list_has_next_page(extracted_pr_first_repo)


def test_get_repo_cursor(extracted_pr_list):
    """
    Check that get_repo_cursor is able to retrieve the end cursor in
    repositories list
    """
    assert (
        get_repo_cursor(extracted_pr_list)
        == 'Y3Vyc29yOnYyOpK5MjAyMC0xMS0wOVQxNDowODo0MSswMTowMM4BZVra'
    )


def test_pr_list_has_next_page(extracted_pr_first_prs):
    """
    Check that the pr_list_has_next_page function
    properly detects a next page in pull requests list
    """
    assert pr_list_has_next_page(extracted_pr_first_prs)


def test_get_pr_cursor(extracted_pr_list):
    """
    Check that get_repo_cursor is able to retrieve the end cursor in
    pull requests list
    """
    assert (
        get_pr_cursor(extracted_pr_list)
        == 'Y3Vyc29yOnYyOpK5MjAyMC0xMS0wOVQxMjoxMzoyMyswMTowMM4e2z5W'
    )


def test_team_list_has_next_page(extracted_teams):
    """
    Check that the pr_list_has_next_page function properly
     detects a next page in teams list
    """
    assert team_list_has_next_page(extracted_teams)


def test_get_team_list_cursor(extracted_teams):
    """
    Check that get_repo_cursor is able to retrieve the end cursor in
    teams list
    """
    assert get_team_list_cursor(extracted_teams) == 'Y3Vyc29yOnYyOpKkQ2FyZc4APmsu'


def test_members_list_has_next_page(extracted_teams):
    """
    Check that the repo_list_has_next_page function properly
    detects a next page in members list
    """
    assert members_list_has_next_page(extracted_teams)


def test_get_members_list_cursor(extracted_teams):
    """
    Check that get_repo_cursor is able to retrieve the end cursor in
    members list
    """
    assert (
        get_members_list_cursor(extracted_teams) == 'Y3Vyc29yOnYyOpKvbGVvY2FyZWwtdG91Y2FuzgQ_Ovs='
    )
