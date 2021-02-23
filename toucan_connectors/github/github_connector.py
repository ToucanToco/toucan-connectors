import asyncio
import logging
import os
from contextlib import suppress
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import requests
from dateutil import relativedelta
from pydantic import Field, create_model
from python_graphql_client import GraphqlClient

from toucan_connectors.common import ConnectorStatus, get_loop
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    DataSlice,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

from .helpers import (
    GithubError,
    KeyNotFoundException,
    RateLimitExhaustedException,
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
    get_rate_limit_info,
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
extraction_start_date = datetime.strftime(
    datetime.now() - relativedelta.relativedelta(years=1), '%Y-%m-%dT%H:%M:%SZ'
)


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
    page_limit: int = Field(10, description='Limit of entries (default is 10 pages)', ge=0)
    entities_limit: int = Field(
        None,
        title='Entities Limit',
        description='Max Number of entities such as teams and repositories to extract',
    )

    @classmethod
    def get_form(cls, connector: 'GithubConnector', current_config, **kwargs):
        """Retrieve a form filled with suggestions of available organizations."""
        # Always add the suggestions for the available organizations
        constraints = {}
        with suppress(Exception):
            available_organizations = connector.get_organizations()
            constraints['organization'] = strlist_to_enum('organization', available_organizations)
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

    def get_organizations(self):
        """Retrieve a list of organizations available to the connector"""

        access_token = self.get_access_token()

        if not access_token:
            raise NoCredentialsError('No credentials')

        headers = {'Authorization': f'Bearer {access_token}'}
        logging.getLogger(__name__).info('Retrieving organization')
        data = requests.get(f'{BASE_ROUTE_REST}user/orgs', headers=headers).json()
        return [str(x['login']) for x in data]

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
        self,
        client: GraphqlClient,
        organization: str,
        dataset: str,
        names=None,
        variables=None,
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
        page_limit: int,
        variables=None,
        data_list=None,
        retrieved_pages=0,
        retries=0,
        retry_limit=2,
        latest_retrieved_object=None,
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
        :param: latest_retrieved_object str of either the latest user name or pull request name retrieved
        in a previous extraction. This will allow to call get_pages to extract only new object (users or pr)
        since the last extraction
        :return: list of extracted data
        """
        if variables is None:
            variables = {}
        if data_list is None:
            data_list = []
        q = queries_funcs_pages[dataset](organization=organization, name=name)
        await asyncio.sleep(0.200)
        data = await client.execute_async(query=q, variables=variables)

        try:
            get_message(data)
            get_errors(data)
            data_value = get_data(data)
            extracted_data = extraction_funcs_pages_1[dataset](
                extraction_funcs_pages_2[dataset](get_organization(data_value))
            )
            page_info = get_page_info(extracted_data)
            formatted_data = format_functions[dataset](extracted_data, name)

            # Check if the extraction script can see a previously extracted object (e.g PR)
            # in current page

            if latest_retrieved_object and dataset == GithubDataSet('pull requests'):
                # check if latest_retrieved_object is in current page

                try:
                    index = [pr['PR Name'] for pr in formatted_data].index(latest_retrieved_object)
                    formatted_data = formatted_data[:index]
                    data_list.extend(formatted_data)
                    return data_list
                except ValueError:
                    pass

            # For now we want to retrieve only max 1 year of Pull requests
            # TODO change this to be able to receive a extraction_start_date date for extraction as a parameter
            if dataset == GithubDataSet('pull requests'):
                try:
                    # Find the first index where PR Creation Date is < extraction_start_date
                    # Throws IndexError if such index cannot be found
                    index = np.where(
                        np.array([pr['PR Creation Date'] for pr in formatted_data])
                        < extraction_start_date
                    )[0][0]
                    formatted_data = formatted_data[:index]
                    data_list.extend(formatted_data)
                    return data_list
                except (IndexError, TypeError):
                    pass

            if dataset == GithubDataSet('pull requests'):
                data_list.extend(formatted_data)
            else:
                data_list.append(formatted_data)

            if has_next_page(page_info) and retrieved_pages < page_limit:
                retrieved_pages += 1
                variables['cursor'] = get_cursor(page_info)
                get_rate_limit_info(data_value)

                await self.get_pages(
                    name=name,
                    client=client,
                    organization=organization,
                    dataset=dataset,
                    variables=variables,
                    data_list=data_list,
                    retrieved_pages=retrieved_pages,
                    page_limit=page_limit,
                    latest_retrieved_object=latest_retrieved_object,
                )

        except GithubError:
            logging.getLogger(__name__).info('Retrying in 15 seconds')
            await asyncio.sleep(15)
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
                    page_limit=page_limit,
                    retry_limit=retry_limit,
                    latest_retrieved_object=latest_retrieved_object,
                )
            else:
                raise GithubError('Max number of retries reached, aborting connection')

        except KeyNotFoundException as k:
            logging.getLogger(__name__).error(f'{k}')

        except RateLimitExhaustedException as r:
            sleep_time = r.args[0]  # Value to wait is sent within the Exception
            logging.getLogger(__name__).info(f'Pausing until reset, waiting {sleep_time}')
            await asyncio.sleep(sleep_time)
            await self.get_pages(
                name=name,
                client=client,
                organization=organization,
                dataset=dataset,
                variables=variables,
                data_list=data_list,
                retrieved_pages=retrieved_pages,
                retries=retries,
                page_limit=page_limit,
                retry_limit=retry_limit,
                latest_retrieved_object=latest_retrieved_object,
            )

        return data_list

    async def _fetch_data(
        self,
        dataset: GithubDataSet,
        organization: str,
        client: GraphqlClient,
        page_limit: int,
        names_limit=None,
        latest_retrieved_object=None,
    ) -> pd.DataFrame:
        """
         Builds the coroutines ran by _retrieve_data
        :param dataset  GithubDataSet, the GithubDataSet to query
        :param organization a str representing the organization from
        which the data will be extracted
        :param client a GraphqlClient that will make the requests to Github's API
        :param page_limit max number of pages to be retrieved by get_pages
        :param names_limit number max of "names" (teams/repos) to extract the data from
        :param latest_retrieved_object a dict with object as key and entity as value e. g {'repo': 'plop', 'pr: stuff'}
        :return: a Pandas DataFrame of pull requests or team memberships
        """
        logging.getLogger(__name__).info(f'Starting fetch for {dataset}')
        names = self.get_names(client=client, organization=organization, dataset=dataset)
        subtasks = [
            self.get_pages(
                name=name,
                client=client,
                dataset=dataset,
                organization=organization,
                page_limit=page_limit,
                latest_retrieved_object=latest_retrieved_object.get(name)
                if latest_retrieved_object
                else None,
            )
            for name in names[:names_limit]
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
            self._fetch_data(
                dataset=dataset,
                organization=organization,
                client=client,
                page_limit=data_source.page_limit,
                names_limit=data_source.entities_limit,
            )
        )

    def get_slice(
        self,
        data_source: GithubDataSource,
        permissions: Optional[dict] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> DataSlice:
        """
        Method to retrieve a part of the data as a pandas dataframe
        and the total size filtered with permissions

        - offset is the index of the starting row
        - limit is the number of pages to retrieve
        Exemple: if offset = 5 and limit = 10 then 10 results are expected from 6th row
        """
        preview_datasource = GithubDataSource(
            page_limit=1,
            dataset=data_source.dataset,
            domain=f'preview_{data_source.domain}',
            name=data_source.name,
            organization=data_source.organization,
            entities_limit=3,
        )
        df = self.get_df(preview_datasource, permissions)
        if limit is not None:
            return DataSlice(df[offset : offset + limit], len(df))
        else:
            return DataSlice(df[offset:], len(df))

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
