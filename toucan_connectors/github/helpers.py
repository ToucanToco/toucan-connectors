from typing import List

import pandas as pd

from toucan_connectors.common import nosql_apply_parameters_to_query


class KeyNotFoundException(Exception):
    """
    Raised when a key is not available in Github's Response
    """


def build_query_pr(organization: str) -> str:
    """

    :param organization: the organization name from which the
    pull requests data will be extracted
    :return: graphql query with the organization name
    """
    return nosql_apply_parameters_to_query(
        """query dataset($cursor_repo: String, $cursor_pr: String) {
      organization(login: "%(organization)s") {
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
    """,
        {'organization': organization},
    )


def build_query_teams(organization: str) -> str:
    """

    :param organization: the organization name from which the
    teams data will be extracted
    :return: graphql query with the organization name
    """
    return nosql_apply_parameters_to_query(
        """query teams($cursor_teams: String, $cursor_members: String) {
      organization(login: "%(organization)s") {
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
""",
        {'organization': organization},
    )


def format_pr_row(repository_name: str, pr_row: dict) -> dict:
    """

    :param repository_name: the repository name from which the
    pull requests data were extracted
    :param pr_row: a dictionary with pull requests data to be formatted
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


def format_team_row(members: dict, team_name: str) -> dict:
    """

    :param members: a list of dict representing a list of members
    :param team_name: a str representing the team name
    :return: a dict with login as key and teams as values
    """
    current_record = {team_name: [dev.get('node').get('login') for dev in members.get('edges')]}
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


def get_data(response: dict) -> dict:
    """

    :param response: a response from Github's API
    :return: the content of the Data field in response if exists
    """
    data = response.get('data')
    if data:
        return data
    else:
        raise KeyNotFoundException('No Data Key Available')


def get_organization(response: dict) -> dict:
    """
    :param response: a response from Github's API
    :return: the content of the organization field in response if exists
    """
    data = get_data(response)
    organization = data.get('organization')
    if organization:
        return organization
    else:
        raise KeyNotFoundException('No Organization Key Available')


def get_repositories(response: dict) -> dict:
    """
    :param response: a response from Github's API
    :return: the content of the repositories field in response if exists
    """
    organization = get_organization(response)
    repositories = organization.get('repositories')
    if repositories:
        return repositories
    else:
        raise KeyNotFoundException('No repositories Key Available')


def get_teams(response):
    """
    :param response: a response from Github's API
    :return: the content of the teams field in response if exists
    """
    organization = get_organization(response)
    teams = organization.get('teams')
    if teams:
        return teams
    else:
        raise KeyNotFoundException('No teams Key Available')


def get_nodes(response: dict) -> List[dict]:
    """
    :param response: a response from Github's API
    :return: the content of the Nodes field in response if exists
    """
    nodes = response.get('nodes')
    return nodes


def get_pull_requests(repo: dict) -> dict:
    """
    :param repo: a repo extracted from Github's API
    :return: the content of the pull_requests field in response if exists
    """
    pull_requests = repo.get('pullRequests')

    if pull_requests:
        return pull_requests
    else:
        raise KeyNotFoundException('No Pull Requests Available')


def get_members(team: dict) -> List[dict]:
    """
    :param team: a team extracted from Github's API
    :return: the content of the members field in response if exists
    """
    members = team.get('members')
    if members:
        return members
    else:
        raise KeyNotFoundException('No Members Available')


def get_page_info(page: dict) -> dict:
    """

    :param page: a page extracted from Github's API
    :return: a dict with pagination data
    """
    page_info = page.get('pageInfo')
    if page_info:
        return page_info
    else:
        raise KeyNotFoundException('No PageInfo Key available')


def has_next_page(page_info: dict) -> bool:
    """

    :param page_info: pagination info
    :return: a bool indicating if response hase a next page
    """
    has_next_page = page_info.get('hasNextPage')

    if has_next_page is None:
        raise KeyNotFoundException('hasNextPage key not available')
    else:
        return has_next_page


def get_cursor(page_info: dict) -> str:
    """

    :param page_info: pagination info
    :return: the endcursor of current page as str
    """
    cursor = page_info.get('endCursor')

    if cursor:
        return cursor
    else:
        raise KeyNotFoundException('endCursor key not available')
