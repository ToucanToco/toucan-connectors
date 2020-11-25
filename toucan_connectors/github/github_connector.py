import asyncio
import logging
import os
from contextlib import suppress
from enum import Enum
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from pydantic import Field, create_model
from python_graphql_client import GraphqlClient

from toucan_connectors.common import ConnectorStatus, get_loop
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

from .helpers import (
    GithubError,
    KeyNotFoundException,
    dataset_formatter,
    extraction_funcs_names,
    extraction_funcs_pages_1,
    extraction_funcs_pages_2,
    extraction_keys,
    format_functions,
    get_cursor,
    get_data,
    get_errors,
    get_message,
    get_nodes,
    get_organization,
    get_page_info,
    has_next_page,
    queries_funcs_names,
    queries_funcs_pages,
)

AUTHORIZATION_URL: str = 'https://github.com/login/oauth/authorize'
SCOPE: str = 'user repo read:org read:discussion'
TOKEN_URL: str = 'https://github.com/login/oauth/access_token'
BASE_ROUTE: str = 'https://api.github.com/graphql'
BASE_ROUTE_REST: str = 'https://api.github.com/'
NO_CREDENTIALS_ERROR = 'No credentials'


class NoCredentialsError(Exception):
    """Raised when no secrets available."""


class GithubDataSet(str, Enum):
    pull_requests = 'pull requests'
    teams = 'teams'


class GithubDataSource(ToucanDataSource):
    dataset: GithubDataSet = GithubDataSet('teams')
    organization: Optional[str] = Field(
        None, title='Organization', description='The organization to extract the data from'
    )

    @classmethod
    def get_form(cls, connector: 'GithubConnector', current_config, **kwargs):
        """Retrieve a form filled with suggestions of available organizations."""
        # Always add the suggestions for the available organizations
        constraints = {}

        with suppress(Exception):
            access_token = connector.get_access_token()

            if not access_token:
                raise NoCredentialsError('No credentials')

            headers = {'Authorization': f'Bearer {access_token}'}
            logging.getLogger(__name__).info('Retrieving organization')
            data = requests.get(f'{BASE_ROUTE_REST}user/orgs', headers=headers).json()
            available_organization = [str(x['login']) for x in data]
            constraints['organization'] = strlist_to_enum('organization', available_organization)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


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
        In the Github's oAuth2 authentication process, client_id & client_secret
        must be sent in the body of the request so we have to set them in
        the mother class. This way they'll be added to her get_access_token method
        """
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def get_names(
        self, client: GraphqlClient, organization: str, dataset: str, names=None, variables=None
    ) -> list:
        """
        Retrieve either repositories names or teams names
        :param client an authenticated python_graphql_client
        :param organization the organization from which team names will be extracted
        :param dataset a GithubDataset the function will retrieve
        :param names a list receiving the names extracted from API
        :variables a dict receiving pagination info

        return: a list of repositories or teams names
        """
        if names is None:
            names = []
        if variables is None:
            variables = {}
        q = queries_funcs_names[dataset](organization=organization)
        data = client.execute(query=q, variables=variables)

        try:
            get_errors(data)
            get_message(data)

            extracted_data = extraction_funcs_names[dataset](get_organization(get_data(data)))
            page_info = get_page_info(extracted_data)
            names.extend([t[extraction_keys[dataset]] for t in get_nodes(extracted_data)])

            if has_next_page(page_info):
                variables['cursor'] = get_cursor(page_info)
                self.get_names(
                    client, organization, names=names, variables=variables, dataset=dataset
                )
        except (GithubError, KeyNotFoundException) as g:
            logging.getLogger(__name__).error(f'Aborting query due to {g}')

        return names

    async def get_pages(
        self,
        name: str,
        client: GraphqlClient,
        organization: str,
        dataset: str,
        variables=None,
        data_list=None,
        page_limit=50,
        retrieved_pages=0,
        retries=0,
        retry_limit=4,
    ) -> List[dict]:
        """
        Extracts pages of either members or pull requests
        :param name a str representing the repo name
        :param client an authenticated python_graphql_client
        :param organization a str representing the organization
        :param dataset a GithubDataset the connector will have to retrieve
        :param: variables dict to store pagination information
        :param: data_list list to store extracted pull requests data
        :param: page_limit pages limit of the extraction
        :param: retrieved_pages int number of pages retrieved
        :param: retries the number of retries done when we got an error
        :param: retry_limit the max number of retries the connector can do
        :return: list of extracted data
        """
        if variables is None:
            variables = {}
        if data_list is None:
            data_list = []
        q = queries_funcs_pages[dataset](organization=organization, name=name)
        data = await client.execute_async(query=q, variables=variables)

        try:
            get_message(data)
            get_errors(data)
            extracted_data = extraction_funcs_pages_1[dataset](
                extraction_funcs_pages_2[dataset](get_organization(get_data(data)))
            )
            page_info = get_page_info(extracted_data)

            if dataset == 'pull requests':
                data_list.extend(format_functions[dataset](extracted_data, name))
            else:
                data_list.append(format_functions[dataset](extracted_data, name))

            if has_next_page(page_info) and retrieved_pages < page_limit:
                retrieved_pages += 1
                variables['cursor'] = get_cursor(page_info)
                await self.get_pages(
                    name=name,
                    client=client,
                    organization=organization,
                    dataset=dataset,
                    variables=variables,
                    data_list=data_list,
                    retrieved_pages=retrieved_pages,
                )

        except GithubError:
            logging.getLogger(__name__).info('Retrying in 1 second')
            await asyncio.sleep(1)
            retries += 1
            if retries <= retry_limit:
                await self.get_pages(
                    name=name,
                    client=client,
                    organization=organization,
                    dataset=dataset,
                    variables=variables,
                    data_list=data_list,
                    retrieved_pages=retrieved_pages,
                    retries=retries,
                )
            else:
                raise GithubError('Max number of retries reached, aborting connection')

        except KeyNotFoundException as k:
            logging.getLogger(__name__).error(f'{k}')

        return data_list

    async def _fetch_data(
        self, dataset: GithubDataSet, organization: str, client: GraphqlClient
    ) -> pd.DataFrame:
        """
         Builds the coroutines ran by _retrieve_data
        :param data_source  GithubDataSource, the GithubDataSource to query
        :param organization a str representing the organization from
        which the data will be extracted
        :param client a GraphqlClient that will make the requests to Github's API
        :return: a Pandas DataFrame of pull requests or team memberships
        """
        logging.getLogger(__name__).info(f'Starting fetch for {dataset}')
        names = self.get_names(client=client, organization=organization, dataset=dataset)
        subtasks = [
            self.get_pages(name=name, client=client, dataset=dataset, organization=organization)
            for name in names
        ]
        unformatted_data = await asyncio.gather(*subtasks)
        return dataset_formatter[dataset]([e for sublist in unformatted_data for e in sublist])

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        """

        :param data_source:  GithubDataSource, the GithubDataSource to query
        :return: a Pandas DataFrame of pull requests or team memberships
        """
        dataset = data_source.dataset
        access_token = self.get_access_token()
        organization = data_source.organization

        if not access_token:
            raise NoCredentialsError(NO_CREDENTIALS_ERROR)

        headers = {'Authorization': f'token {access_token}'}
        client = GraphqlClient(BASE_ROUTE, headers)
        loop = get_loop()
        return loop.run_until_complete(
            self._fetch_data(dataset=dataset, organization=organization, client=client)
        )

    def get_status(self) -> ConnectorStatus:
        """
        Test the Github's connexion.
        :return: a ConnectorStatus with the current status
        """
        try:
            access_token = self.get_access_token()
            if access_token:
                c = ConnectorStatus(status=True)
                return c
            else:
                return ConnectorStatus(status=False)
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')
