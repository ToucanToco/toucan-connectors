import logging
import os
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import Field
from python_graphql_client import GraphqlClient

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

from .helpers import (
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

AUTHORIZATION_URL: str = 'https://github.com/login/oauth/authorize'
SCOPE: str = 'user%20repo'
TOKEN_URL: str = 'https://github.com/login/oauth/access_token'
BASE_ROUTE: str = 'https://github.com/'
NO_CREDENTIALS_ERROR = 'No credentials'


class GithubError(Exception):
    """Raised when we receive an error message
    from Github's API
    """


class NoCredentialsError(Exception):
    """Raised when no secrets available."""


class GithubDataSet(str, Enum):
    pull_requests = 'pull requests'
    teams = 'teams'


class GithubDataSource(ToucanDataSource):
    dataset: GithubDataSet = 'pull requests'
    organization: str = Field(..., description='The organization to extract the data from')


class GithubConnector(ToucanConnector):
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]
    data_source_model: GithubDataSource

    @staticmethod
    def get_connector_secrets_form() -> ConnectorSecretsForm:
        return ConnectorSecretsForm(
            documentation_md=(Path(os.path.dirname(__file__)) / 'doc.md').read_text(),
            secrets_schema=OAuth2ConnectorConfig.schema(),
        )

    def __init__(self, **kwargs):
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=SCOPE,
            token_url=TOKEN_URL,
            secrets_keeper=kwargs['secrets_keeper'],
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
        )

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        """
        In the Aircall oAuth2 authentication process, client_id & client_secret
        must be sent in the body of the request so we have to set them in
        the mother class. This way they'll be added to her get_access_token method
        """
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def extract_pr_data(
        self,
        client,
        organization: str,
        repo_rows=None,
        variables=None,
        pr_page_limit=10,
        retrieved_pages=0,
    ) -> pd.DataFrame:
        """
        Extract the data needed to build the PR dataframe
        :param organization: str, the organization's name as defined in Github
        :param repo_rows: defaulted at None, placeholder for a list of Pull Requests
        :param variables: a dict containing the variables for pagination
        :param pr_page_limit: max number of pull requests pages to retrieve
        :param retrieved_pages: number of page retrived, incremented at during each iteration
        :return: a Pandas DataFrame built from the list of pull requests
        """
        query = build_query_pr(organization)

        if variables is None:
            variables = {}
        if repo_rows is None:
            repo_rows = []

        data = client.execute(query=query, variables=variables)
        logging.getLogger(__file__).info(
            f'Request sent to Github ' f'for page {retrieved_pages} ' f'and for pull requests data'
        )
        errors = data.get('errors')

        if errors:
            logging.getLogger(__file__).error(f'A Github error occured:' f' {errors}')
            raise GithubError(f'Aborting query due to {errors}')

        repo_rows.extend(self.build_pr_rows(data))

        if pr_list_has_next_page(data) and retrieved_pages < pr_page_limit:
            retrieved_pages += 1
            variables['cursor_pr'] = get_pr_cursor(data)
            self.extract_pr_data(
                client=client,
                organization=organization,
                repo_rows=repo_rows,
                variables=variables,
                retrieved_pages=retrieved_pages,
                pr_page_limit=pr_page_limit,
            )

        elif repo_list_has_next_page(data):
            variables['cursor_pr'] = None
            variables['cursor_repo'] = get_repo_cursor(data)
            self.extract_pr_data(
                client=client,
                organization=organization,
                repo_rows=repo_rows,
                variables=variables,
                pr_page_limit=pr_page_limit,
            )

        return pd.DataFrame(repo_rows)

    def build_pr_rows(self, extracted_data: dict) -> list:
        """
        Builds a list of rows containing all the PR Data

        :param extracted_data: json response from Github's API
        :return: a list of formatted pull request rows
        """
        rows = []
        for repository_node in (
            extracted_data.get('data').get('organization').get('repositories').get('nodes')
        ):
            if len(repository_node.get('pullRequests').get('nodes')) > 0:

                for PR in repository_node.get('pullRequests').get('nodes'):
                    rows.append(format_pr_row(repository_node['name'], PR))

            else:
                rows.append({'Repo Name': repository_node['name']})
        return rows

    def extract_teams_data(
        self,
        client,
        organization,
        members_page_limit=10,
        teams_rows=None,
        retrieved_pages=0,
        variables=None,
    ) -> pd.DataFrame:
        """
        :param organization:  str, the organization's name as defined in Github
        :param members_page_limit: max number of members pages to retrieve
        :param teams_rows: defaulted at None, placeholder for a list of teams
        :param retrieved_pages: number of page retrived, incremented at during each iteration
        :param variables: a dict containing the variables for pagination
        :return: a Pandas DataFrame built from the list of teams
        """
        if variables is None:
            variables = {}
        if teams_rows is None:
            teams_rows = []

        query = build_query_teams(organization)
        data = client.execute(query=query, variables=variables)
        logging.getLogger(__file__).info(
            f'Request sent to Github ' f'for page {retrieved_pages} ' f'and for teams data'
        )
        errors = data.get('errors')

        if errors:
            logging.getLogger(__file__).error(f'A Github ' f'error occured: {errors}')
            raise GithubError(f'Aborting query due to {errors}')

        teams_rows.append(self.build_team_dict(data))

        if members_list_has_next_page(data) and retrieved_pages < members_page_limit:
            retrieved_pages += 1
            variables = {'cursor_members': get_members_list_cursor(data)}
            self.extract_teams_data(
                client=client,
                organization=organization,
                teams_rows=teams_rows,
                retrieved_pages=retrieved_pages,
                variables=variables,
                members_page_limit=members_page_limit,
            )

        elif team_list_has_next_page(data):
            variables = {'cursor_teams': get_team_list_cursor(data), 'cursor_member': None}
            self.extract_teams_data(
                client=client,
                organization=organization,
                teams_rows=teams_rows,
                variables=variables,
                members_page_limit=members_page_limit,
            )
        return format_team_df(teams_rows)

    def build_team_dict(self, extracted_data: dict) -> dict:
        """

        :param extracted_data: a dict with teams
        data extracted from Github's API
        :return: a dict with developers names as key and
        an array of team names as values
        """
        team_node = extracted_data.get('data').get('organization').get('teams').get('nodes')[0]
        return format_team_row(team_node)

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        """

        :param organization:  str, the organization's name as defined in Github
        :param data_source:  GithubDataSource, the GithubDataSource to query
        :return: a Pandas DataFrame of merged, pull requests & teams
        """
        dataset = data_source.dataset
        organization = data_source.organization
        access_token = self.get_access_token()

        if not access_token:
            raise NoCredentialsError(NO_CREDENTIALS_ERROR)

        headers = {'Authorization': f'token {access_token}'}
        client = GraphqlClient(BASE_ROUTE, headers)

        if dataset == 'pull requests':
            return self.extract_pr_data(client=client, organization=organization)
        elif dataset == 'teams':
            return self.extract_teams_data(client=client, organization=organization)

    def get_status(self) -> ConnectorStatus:
        """
        Test the Github's connexion.
        """
        try:
            access_token = self.get_access_token()
            if access_token:
                c = ConnectorStatus(status=True)
                return c
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')
        if not access_token:
            return ConnectorStatus(status=False, error='Credentials are missing')
