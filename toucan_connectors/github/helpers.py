from typing import List

import pandas as pd


def build_query_pr(organization: str) -> str:
    """

    :param organization: the organization name from which the
    pull requests data will be extracted
    :return: graphql query with the organization name
    """
    query = (
        """
    query dataset($cursor_repo: String, $cursor_pr: String) {
      organization(login: \""""
        + organization
        + """\") {
        repositories(first: 1, orderBy: {field: PUSHED_AT, direction: DESC},
         after: $cursor_repo) {
          nodes {
            name
            pullRequests(orderBy: {field: CREATED_AT, direction: DESC},
             first: 90, after: $cursor_pr) {
              nodes {
                createdAt
                mergedAt
                deletions
                additions
                title
                state
                labels(orderBy: {field: NAME, direction: ASC}, last: 10) {
                  edges {
                    node {
                      name
                    }
                  }
                }
                commits(first: 1) {
                  edges {
                    node {
                      commit {
                        author {
                          user {
                            login
                          }
                        }
                      }
                    }
                  }
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    }
    """
    )
    return query


def build_query_teams(organization: str) -> str:
    """

    :param organization: the organization name from which the
    teams data will be extracted
    :return: graphql query with the organization name
    """
    query = (
        """query teams($cursor_teams: String, $cursor_members: String) {
  organization(login: \""""
        + organization
        + """\") {
    teams(first: 1, orderBy: {field: NAME, direction: ASC},
     after: $cursor_teams) {
      nodes {
        name
        members(first: 100, orderBy: {field: LOGIN, direction: ASC},
         after: $cursor_members) {
          edges {
            node {
              login
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
      pageInfo {
        endCursor
        hasNextPage
      }
    }
  }
}
"""
    )
    return query


def format_pr_row(repository_name: str, pr_row: dict) -> dict:
    """

    :param organization: the organization name from which the
    pull requests data were extracted
           pr_row: a dictionary with pull requests data to be formatted
    :return: a formatted dict with pull requests data
    """
    current_record = {}
    current_record['Repo Name'] = repository_name
    current_record['PR Name'] = pr_row.get('title')
    current_record['PR Creation Date'] = pr_row.get('createdAt')
    current_record['PR Merging Date'] = pr_row.get('mergedAt')
    current_record['PR Additions'] = pr_row.get('additions')
    current_record['PR Deletions'] = pr_row.get('deletions')
    current_record['PR Type'] = [
        label.get('node').get('name') for label in pr_row.get('labels').get('edges')
    ]
    if pr_row.get('commits'):
        user = (
            pr_row.get('commits')
            .get('edges')[0]
            .get('node')
            .get('commit')
            .get('author')
            .get('user')
        )
        if user:
            current_record['Dev'] = user.get('login')
        else:
            current_record['Dev'] = None
    else:
        current_record['Dev'] = None
    return current_record


def format_team_row(team_row: dict) -> dict:
    """

    :param team_row: a dict with team as key and logins as values
    :return: a dict with login as key and teams as values
    """
    current_record = {}
    current_record[team_row['name']] = [
        dev.get('node').get('login') for dev in team_row.get('members').get('edges')
    ]
    devs = pd.DataFrame(current_record).melt()
    devs.set_index('value', drop=True, inplace=True)
    return devs.to_dict().get('variable')


def format_team_df(team_rows: List[dict]) -> pd.DataFrame:
    """

    :param team_rows: a list of dict with login as key and list
     of teams as value
    :return: a formatted pandas DataFrame with login in dev column and
    list of teams in teams column
    """
    team_df = pd.DataFrame(team_rows).transpose()
    team_df['teams'] = team_df.values.tolist()
    team_df['teams'] = team_df['teams'].apply(lambda x: list({t for t in x if not pd.isnull(t)}))
    team_df.reset_index(inplace=True)
    team_df.rename(columns={'index': 'Dev'}, inplace=True)
    return team_df[['Dev', 'teams']]


def repo_list_has_next_page(repoPages: dict) -> bool:
    """

    :param repoPages: extracted data of repositories
    :return: a boolean indicating if the repository data extracted has
    another page
    """
    return (
        repoPages.get('data')
        .get('organization')
        .get('repositories')
        .get('pageInfo')
        .get('hasNextPage')
    )


def pr_list_has_next_page(repoPages: dict) -> bool:
    """

    :param repoPages: extracted data of pull requests
    :return: a boolean indicating if the pull requests data extracted has
    another page
    """
    return (
        repoPages.get('data')
        .get('organization')
        .get('repositories')
        .get('nodes')[0]
        .get('pullRequests')
        .get('pageInfo')
        .get('hasNextPage')
    )


def get_pr_cursor(repoPages: dict) -> str:
    """

    :param repoPages: extracted data of pull requests
    :return: the cursor to the next pull request page as str
    """
    return (
        repoPages.get('data')
        .get('organization')
        .get('repositories')
        .get('nodes')[0]
        .get('pullRequests')
        .get('pageInfo')
        .get('endCursor')
    )


def get_repo_cursor(repoPages: dict) -> str:
    """

    :param repoPages: extracted data of pull requests
    :return: the cursor to the next repository page as str
    """
    return (
        repoPages.get('data')
        .get('organization')
        .get('repositories')
        .get('pageInfo')
        .get('endCursor')
    )


def team_list_has_next_page(teamPages: dict) -> bool:
    """

    :param teamPages: extracted data of teams
    :return: a boolean indicating if the teams data extracted has
    another page
    """
    return teamPages.get('data').get('organization').get('teams').get('pageInfo').get('hasNextPage')


def get_team_list_cursor(teampages: dict) -> str:
    """

    :param teamPages: extracted data of teams
    :return: the cursor to the next team page as str
    """
    return teampages.get('data').get('organization').get('teams').get('pageInfo').get('endCursor')


def members_list_has_next_page(teamPages: dict) -> bool:
    """

    :param teamPages: extracted data of teams
    :return: a boolean indicating if the members data extracted has
    another page
    """
    return (
        teamPages.get('data')
        .get('organization')
        .get('teams')
        .get('nodes')[0]
        .get('members')
        .get('pageInfo')
        .get('hasNextPage')
    )


def get_members_list_cursor(teamPages: dict) -> str:
    """

    :param teamPages: extracted data of teams
    :return: the cursor to the next members page as str
    """
    return (
        teamPages.get('data')
        .get('organization')
        .get('teams')
        .get('nodes')[0]
        .get('members')
        .get('pageInfo')
        .get('endCursor')
    )
