import logging
from datetime import datetime
from typing import List

import pandas as pd

from toucan_connectors.common import nosql_apply_parameters_to_query


class GithubError(Exception):
    """Raised when we receive an error message
    from Github's API
    """


class KeyNotFoundException(Exception):
    """
    Raised when a key is not available in Github's Response
    """


def build_query_repositories(organization: str) -> str:
    """
    Builds the GraphQL query to retrieve a list of repositories
    from Github's API
    :param organization: the organization name from which the
    repositories data will be extracted
    :return: graphql query with the sanitized organization name
    """
    return nosql_apply_parameters_to_query(
        """query repositories($cursor: String) {
          organization(login: "%(organization)s") {
            repositories(first: 90, orderBy: {field: PUSHED_AT, direction: DESC},
             after: $cursor) {
              nodes {
                name
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
        }
    }""",
        {'organization': organization},
    )


def build_query_pr(organization: str, name: str) -> str:
    """
    Builds the GraphQL query to retrieve a list of pull requests
    from Github's API
    :param organization: the organization name from which the
    pull requests data will be extracted
    :param name a str representing the repository to extract the PRs from
    :return: graphql query with the sanitized organization name
    """
    return nosql_apply_parameters_to_query(
        """query pr($cursor: String) {
          organization(login: "%(organization)s") {
            repository(name: "%(repo_name)s") {
                name
                pullRequests(orderBy: {field: CREATED_AT, direction: DESC},
                 first: 100, after: $cursor) {
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
              }
              rateLimit {
                remaining
                resetAt
              }
            }""",
        {'organization': organization, 'repo_name': name},
    )


def build_query_teams(organization: str) -> str:
    """
    Builds the GraphQL query to retrieve a list of teams
    from Github's API
    :param organization: the organization name from which the
    teams data will be extracted
    :return: graphql query with the sanitized organization name
    """
    return nosql_apply_parameters_to_query(
        """query teams($cursor: String) {
              organization(login: "%(organization)s") {
                teams(first: 90, orderBy: {field: NAME, direction: ASC},
                 after: $cursor) {
                  nodes {
                    slug
                  }
                  pageInfo {
                    endCursor
                    hasNextPage
                  }
                }
              }
              rateLimit {
                remaining
                resetAt
              }
            }
            """,
        {'organization': organization},
    )


def build_query_members(organization: str, name: str) -> str:
    """
    Builds the GraphQL query to retrieve a list of team members
    from Github's API
    :param organization: the organization name from which the
    members data will be extracted
    :param team the team name from which the
    members data will be extracted
    :return: graphql query with sanitized organization and team names
    """
    return nosql_apply_parameters_to_query(
        """
    query members($cursor: String){
      organization(login: "%(organization)s") {
        team(slug: "%(team)s"){
            members(first: 100, orderBy: {field: LOGIN, direction: ASC},
             after: $cursor) {
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
      }
}
""",
        {'organization': organization, 'team': name},
    )


def format_pr_row(pr_row: dict):
    """
    Extracts and formats data extracted from a pull request
    :param pr_row: a dictionary with a pull request data to be formatted
    :return: a formatted dict with pull requests data
    """

    current_record = {}
    current_record['PR Name'] = pr_row.get('title')
    current_record['PR Creation Date'] = pr_row.get('createdAt')
    current_record['PR Merging Date'] = pr_row.get('mergedAt')
    current_record['PR Additions'] = pr_row.get('additions')
    current_record['PR Deletions'] = pr_row.get('deletions')
    try:

        current_record['PR Type'] = [
            label['node'].get('name') for label in pr_row['labels'].get('edges')
        ]
        edges = get_edges(pr_row.get('commits'))
        # Here we choose to select the author of the last commit as the author of the PR
        # It's less wrong than choosing the author of the first commit in case of rebase
        current_record['Dev'] = edges[-1]['node']['commit']['author']['user']['login']

    except (AttributeError, TypeError, KeyNotFoundException):
        current_record['Dev'] = None

    return current_record


def format_pr_rows(pr_nodes: dict, repo_name: str) -> List[dict]:
    """
    A wrapper function to multiprocess the pull requests formatting
    :param pr_rows: pull requests extracted from Github's API
    :param repo_name: a str representing the repository's name
    :return: a formatted dict with pull requests data
    """

    pull_requests = get_nodes(pr_nodes)
    formatted = [
        format_pr_row(pr)
        for pr in pull_requests
        if pr.get('state') != 'CLOSED'  # we ignore closed PR in KPIs
    ]
    return [dict(r, **{'Repo Name': repo_name}) for r in formatted]


def format_team_row(members: dict, team_name: str) -> dict:
    """
    Extracts and formats data extracted from a team
    :param members: a dict representing a list of members
    :param team_name: a str representing the team name
    :return: a dict with login as key and teams as values
    """
    current_record = {team_name: [dev.get('node').get('login') for dev in get_edges(members)]}
    devs = pd.DataFrame(current_record).melt()
    devs.set_index('value', drop=True, inplace=True)
    return devs.to_dict().get('variable')


def format_team_df(team_rows: List[dict]) -> pd.DataFrame:
    """
    Builds a Pandas DataFrame from members rows.
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
    Extracts value from a dict with data key or raises an error if the key is not available
    :param response: a response from Github's API
    :return: the content of the Data field in response if exists
    """
    data = response.get('data')
    if data:
        return data
    else:
        raise KeyNotFoundException('No Data Key Available')


def get_organization(data: dict) -> dict:
    """
    Extracts value from a dict with organization key or raises an error if the key is not available
    :param data: data extracted from Github's API
    :return: the content of the organization field in response if exists
    """
    organization = data.get('organization')
    if organization:
        return organization
    else:
        raise KeyNotFoundException('No Organization Key Available')


def get_repositories(organization: dict) -> dict:
    """
    Extracts value from a dict with repositories key or raises
    an error if the key is not available
    :param organization: an organization extracted from Github's API
    :return: the content of the repositories field in response if exists
    """

    repositories = organization.get('repositories')
    if repositories:
        return repositories
    else:
        raise KeyNotFoundException('No repositories Key Available')


def get_repository(organization: dict) -> dict:
    """
    Extracts value from a dict with organization key or raises an error if the key is not available
    :param organization: an organization extracted from Github's API
    :return: the content of the repository field in response if exists
    """
    repository = organization.get('repository')
    if repository:
        return repository
    else:
        raise KeyNotFoundException('No repository Key Available')


def get_teams(organization: dict):
    """
    Extracts value from a dict with teams key or raises an error if the key is not available
    :param organization: an organization extracted from Github's API
    :return: the content of the teams field in response if exists
    """
    teams = organization.get('teams')
    if teams:
        return teams
    else:
        raise KeyNotFoundException('No teams Key Available')


def get_nodes(response: dict) -> List[dict]:
    """
    Extracts value from a dict with nodes key or raises an error if the key is not available
    :param response: a response from Github's API
    :return: the content of the Nodes field in response if exists
    """
    nodes = response.get('nodes')
    return nodes


def get_edges(data: dict) -> List[dict]:
    """
    Extracts value from a dict with edges key or raises an error if the key is not available
    :param data: data extracted from Github's API
    :return: the content of the Edges field in response if exists
    """
    edges = data.get('edges')
    if edges:
        return edges
    else:
        raise KeyNotFoundException('No Edges Key Available')


def get_pull_requests(repo: dict) -> dict:
    """
    Extracts value from a dict with pullRequests key or raises an error if the key is not available
    :param repo: a repo extracted from Github's API
    :return: the content of the pull_requests field in response if exists
    """
    pull_requests = repo.get('pullRequests')

    if pull_requests:
        return pull_requests
    else:
        raise KeyNotFoundException('No Pull Requests Available')


def get_team(organization: dict) -> dict:
    """
    Extracts value from a dict with team key or raises an error if the key is not available
    :param organization: organization data extracted from Github's API
    :return: the content of the team field in response if exists
    """
    team = organization.get('team')
    if team:
        return team
    else:
        raise KeyNotFoundException


def get_members(team: dict) -> dict:
    """
    Extracts value from a dict with members key or raises an error if the key is not available
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
    Extracts value from a dict with pageInfo key or raises an error if the key is not available
    :param page: a page extracted from Github's API
    :return: a dict with pagination data
    """
    page_info = page.get('pageInfo')
    if page_info:
        return page_info
    else:
        raise KeyNotFoundException('No PageInfo Key available')


def get_errors(data: dict):
    """
    Extracts value from a dict with errors key, log the errors if they exist and raises
    a GithubError
    :param dict: data extracted from Github's API
    """
    errors = data.get('errors')
    if errors:
        for error in errors:
            logging.getLogger(__name__).error(f'A Github error occured:' f' {error}')
        raise GithubError(f'Retrying query due to {errors}')


def has_next_page(page_info: dict) -> bool:
    """
    Extracts value from a dict with hasNextPage key, raises an error if the key is not available
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
    Extracts value from a dict with endCursor key and or raises an error if the key is not available
    :param page_info: pagination info
    :return: the endcursor of current page as str
    """
    cursor = page_info.get('endCursor')

    if cursor:
        return cursor
    else:
        raise KeyNotFoundException('endCursor key not available')


def get_message(response: dict):
    """
    Extracts value from a dict with message key, log an error if the there is a message
    and raises a GithubError
    :param response: response from Github's API
    :return: extracted message
    """
    message = response.get('message')

    if message:
        logging.getLogger(__name__).error(f'A Github error occured:' f' {message}')
        raise GithubError(f'API sent {message}')


class RateLimitExhaustedException(Exception):
    """Raised when remaining resources from API is 0"""


def get_rate_limit_info(response: dict):
    """
    Extract rate limit info from response.
    If remaining resources is 0 then throw an exception with
    reset time.
    rate_limit part is like this:
        "rateLimit": {
      "remaining": 4994,
      "resetAt": "2021-02-23T13:26:47Z" UTC TIME !
    """
    rate_limit_info = response.get('rateLimit')
    if rate_limit_info:
        if rate_limit_info['remaining'] < 100:  # Raise the Exception Before reaching the limit
            logging.getLogger(__name__).info('Rate limit exhausted')
            resetAt_date = datetime.strptime(rate_limit_info['resetAt'], '%Y-%m-%dT%H:%M:%SZ')
            timetowait = (resetAt_date - datetime.utcnow()).seconds + 1  # adding 1 sec to round up
            raise RateLimitExhaustedException(timetowait)


extraction_funcs_names = {'pull requests': get_repositories, 'teams': get_teams}
extraction_funcs_pages_1 = {'pull requests': get_pull_requests, 'teams': get_members}
extraction_funcs_pages_2 = {'pull requests': get_repository, 'teams': get_team}
queries_funcs_names = {'pull requests': build_query_repositories, 'teams': build_query_teams}
queries_funcs_pages = {'pull requests': build_query_pr, 'teams': build_query_members}
extraction_keys = {'pull requests': 'name', 'teams': 'slug'}
format_functions = {'pull requests': format_pr_rows, 'teams': format_team_row}
dataset_formatter = {'pull requests': pd.DataFrame, 'teams': format_team_df}
