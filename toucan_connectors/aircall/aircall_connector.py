import asyncio
import logging
import os
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from aiohttp import ClientSession
from pydantic import Field

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
)

from .constants import MAX_RUNS, PER_PAGE
from .helpers import DICTIONARY_OF_FORMATTERS, build_df, build_empty_df

AUTHORIZATION_URL: str = 'https://dashboard-v2.aircall.io/oauth/authorize'
SCOPE: str = 'public_api'
TOKEN_URL: str = 'https://api.aircall.io/v1/oauth/token'
BASE_ROUTE: str = 'https://api.aircall.io/v1'
NO_CREDENTIALS_ERROR = 'No credentials'


class AircallRateLimitExhaustedException(Exception):
    """Raised when the extraction reached the max amount of request"""


class NoCredentialsError(Exception):
    """Raised when no secrets avaiable."""


class AircallDataset(str, Enum):
    calls = 'calls'
    tags = 'tags'
    users = 'users'


async def fetch_page(
    dataset: str,
    data_list: List[dict],
    session: ClientSession,
    limit,
    current_pass: int,
    new_page=1,
    delay_counter=0,
    *,
    query_params=None,
) -> List[dict]:
    """
    Fetches data from AirCall API

    dependent on existence of other pages and call limit
    """
    endpoint = f'{BASE_ROUTE}/{dataset}?per_page={PER_PAGE}&page={new_page}'
    try:
        if query_params:
            data: dict = await fetch(endpoint, session, query_params)
        else:
            data: dict = await fetch(endpoint, session)

        logging.getLogger(__name__).info(
            f'Request sent to Aircall for page {new_page} for dataset {dataset}'
        )

        aircall_error = data.get('error')
        if aircall_error:
            logging.getLogger(__name__).error(f'Aircall error has occurred: {aircall_error}')
            delay_timer = 1
            max_num_of_retries = 3
            await asyncio.sleep(delay_timer)
            if delay_counter < max_num_of_retries:
                delay_counter += 1
                logging.getLogger(__name__).info('Retrying Aircall API')
                data_list = await fetch_page(
                    dataset,
                    data_list,
                    session,
                    limit,
                    current_pass,
                    new_page,
                    delay_counter,
                    query_params=query_params,
                )
            else:
                logging.getLogger(__name__).error('Aborting Aircall requests')
                raise AircallException(f'Aborting Aircall requests due to {aircall_error}')

        delay_counter = 0
        data_list.append(data)

        next_page_link = None
        meta_data = data.get('meta')
        if meta_data is not None:
            next_page_link: Optional[str] = meta_data.get('next_page_link')

        if limit > -1:
            current_pass += 1

            if next_page_link is not None and current_pass < limit:
                next_page = meta_data['current_page'] + 1
                data_list = await fetch_page(
                    dataset,
                    data_list,
                    session,
                    limit,
                    current_pass,
                    next_page,
                    query_params=query_params,
                )
        else:
            if next_page_link is not None:
                next_page = meta_data['current_page'] + 1
                data_list = await fetch_page(
                    dataset,
                    data_list,
                    session,
                    limit,
                    current_pass,
                    next_page,
                    query_params=query_params,
                )

    except AircallRateLimitExhaustedException as a:
        reset_timestamp = int(a.args[0])
        delay = reset_timestamp - (int(datetime.timestamp(datetime.utcnow())) + 1)
        logging.getLogger(__name__).info(f'Rate limit reached, pausing {delay} seconds')
        time.sleep(delay)
        logging.getLogger(__name__).info('Extraction restarted')
        data_list = await fetch_page(
            dataset,
            data_list,
            session,
            limit,
            current_pass,
            new_page,
            delay_counter,
            query_params=query_params,
        )

    return data_list


async def fetch(new_endpoint, session: ClientSession, query_params=None) -> dict:
    """The basic fetch function"""
    async with session.get(new_endpoint, params=query_params) as res:
        try:
            rate_limit_reset = res.headers['X-AircallApi-Reset']
            raise AircallRateLimitExhaustedException(rate_limit_reset)
        except KeyError:
            pass
        return await res.json()


class AircallDataSource(ToucanDataSource):
    limit: int = Field(MAX_RUNS, description='Limit of entries (default is 1 run)', ge=-1)
    dataset: AircallDataset = 'calls'


class AircallConnector(ToucanConnector):
    """
    This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    using oAuth2 for authentication
    """

    _auth_flow = 'oauth2'
    provided_token: Optional[str]
    auth_flow_id: Optional[str]
    data_source_model: AircallDataSource
    _oauth_trigger = 'instance'
    oauth2_version = Field('1', **{'ui.hidden': True})

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
        self.provided_token = kwargs.get('provided_token', None)

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
        if self.provided_token:
            return self.provided_token
        return self.__dict__['_oauth2_connector'].get_access_token()

    async def _fetch(self, url, headers=None, query_params=None):
        """Build the final request along with headers."""
        async with ClientSession(headers=headers) as session:
            return await fetch(url, session, query_params=query_params)

    def _run_fetch(self, url):
        """Run loop."""
        access_token = self.get_access_token()
        if not access_token:
            raise NoCredentialsError(NO_CREDENTIALS_ERROR)
        headers = {'Authorization': f'Bearer {access_token}'}

        loop = get_loop()
        future = asyncio.ensure_future(self._fetch(url, headers))
        return loop.run_until_complete(future)

    async def _get_data(
        self, dataset: str, limit, query_params=None
    ) -> Tuple[List[dict], List[dict]]:
        """Triggers fetches for data and does preliminary filtering process"""
        access_token = self.get_access_token()
        if not access_token:
            raise NoCredentialsError(NO_CREDENTIALS_ERROR)
        headers = {'Authorization': f'Bearer {access_token}'}

        async with ClientSession(headers=headers) as session:
            team_data, variable_data = await asyncio.gather(
                fetch_page(
                    'teams',
                    [],
                    session,
                    limit,
                    0,
                    query_params=None,  # for now we don't provide param while querying the teams endpoint
                ),
                fetch_page(dataset, [], session, limit, 0, query_params=query_params),
            )
            team_response_list = []
            variable_response_list = []
            if len(team_data) > 0:
                for data in team_data:
                    for team_obj in data['teams']:
                        team_response_list += DICTIONARY_OF_FORMATTERS['teams'](team_obj)
            if len(variable_data) > 0:
                for data in variable_data:
                    variable_response_list += [
                        DICTIONARY_OF_FORMATTERS.get(dataset, 'users')(obj) for obj in data[dataset]
                    ]
            return team_response_list, variable_response_list

    async def _get_tags(self, dataset: str, limit) -> List[dict]:
        """Triggers fetches for tags and does preliminary filtering process"""
        access_token = self.get_access_token()
        if not access_token:
            raise NoCredentialsError(NO_CREDENTIALS_ERROR)
        headers = {'Authorization': f'Bearer {access_token}'}

        async with ClientSession(headers=headers) as session:
            raw_data = await fetch_page(
                dataset,
                [],
                session,
                limit,
                1,
            )
            tags_data_list = []
            for data in raw_data:
                tags_data_list += data['tags']
            return tags_data_list

    def run_fetches(self, dataset, limit, query_params=None) -> Tuple[List[dict], List[dict]]:
        """sets up event loop and fetches for 'calls' and 'users' datasets"""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_data(dataset, limit, query_params))
        return loop.run_until_complete(future)

    def run_fetches_for_tags(self, dataset, limit):
        """sets up event loop and fetches for 'tags' dataset"""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_tags(dataset, limit))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: AircallDataSource, query_params=None) -> pd.DataFrame:
        """retrieves data from AirCall API"""
        dataset = data_source.dataset
        empty_df = build_empty_df(dataset)

        # NOTE: no check needed on limit here because a non-valid limit
        # raises a Pydantic ValidationError
        limit = data_source.limit

        if dataset == 'tags':
            non_empty_df = pd.DataFrame([])
            if limit != 0:
                res = self.run_fetches_for_tags(dataset, limit)
                non_empty_df = pd.DataFrame(res)
            return pd.concat([empty_df, non_empty_df])
        else:
            team_data = pd.DataFrame([])
            variable_data = pd.DataFrame([])
            if limit != 0:
                team_data, variable_data = self.run_fetches(dataset, limit, query_params)

            return build_df(
                dataset, [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)]
            )

    def get_status(self) -> ConnectorStatus:
        """
        Test the Aircall connexion.
        """
        try:
            access_token = self.get_access_token()
            if access_token:
                return ConnectorStatus(status=True)
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')
        if not access_token:
            return ConnectorStatus(status=False, error='Credentials are missing')

    def get_slice(
        self,
        data_source: AircallDataSource,
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
        preview_datasource = AircallDataSource(
            limit=1,
            dataset=data_source.dataset,
            domain=f'preview_{data_source.domain}',
            name=data_source.name,
        )
        df = self.get_df(preview_datasource, permissions)
        if limit is not None:
            return DataSlice(df[offset : offset + limit], len(df))
        else:
            return DataSlice(df[offset:], len(df))


class AircallException(Exception):
    """Raised when an error occured when querying Aircall's API"""
