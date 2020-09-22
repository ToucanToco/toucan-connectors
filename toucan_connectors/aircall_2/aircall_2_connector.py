import asyncio
import logging
from enum import Enum
from typing import List, Optional, Tuple

import pandas as pd
from aiohttp import ClientSession
from pydantic import Field

from toucan_connectors.aircall.constants import MAX_RUNS, PER_PAGE
from toucan_connectors.aircall.helpers import DICTIONARY_OF_FORMATTERS, build_df, build_empty_df
from toucan_connectors.common import get_loop
from toucan_connectors.secrets_common import retrieve_secrets_from_kwargs
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

BASE_ROUTE = 'https://api.aircall.io/v1'


async def fetch_page(
    dataset: str,
    data_list: List[dict],
    session: ClientSession,
    limit,
    current_pass: int,
    new_page=1,
    delay_counter=0,
) -> List[dict]:
    """
    Fetches data from AirCall API

    dependent on existence of other pages and call limit
    """
    endpoint = f'{BASE_ROUTE}/{dataset}?per_page={PER_PAGE}&page={new_page}'
    data: dict = await fetch(endpoint, session)
    logging.getLogger(__file__).info(
        f'Request sent to Aircall for page {new_page} for dataset {dataset}'
    )

    aircall_error = data.get('error')
    if aircall_error:
        logging.getLogger(__file__).error(f'Aircall error has occurred: {aircall_error}')
        delay_timer = 1
        max_num_of_retries = 3
        await asyncio.sleep(delay_timer)
        if delay_counter < max_num_of_retries:
            delay_counter += 1
            logging.getLogger(__file__).info('Retrying Aircall API')
            data_list = await fetch_page(
                dataset, data_list, session, limit, current_pass, new_page, delay_counter
            )
        else:
            logging.getLogger(__file__).error('Aborting Aircall requests')
            raise AircallError(f'Aborting Aircall requests due to {aircall_error}')

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
                dataset, data_list, session, limit, current_pass, next_page
            )
    else:
        if next_page_link is not None:
            next_page = meta_data['current_page'] + 1
            data_list = await fetch_page(
                dataset, data_list, session, limit, current_pass, next_page
            )

    return data_list


async def fetch(new_endpoint, session: ClientSession) -> dict:
    """The basic fetch function"""
    async with session.get(new_endpoint) as res:
        return await res.json()


class AircallError(Exception):
    """Raised when an error is returned by Aircall."""


class AircallDataset(str, Enum):
    """An enum containing the possible Aircall resources used in requests."""

    calls = 'calls'
    tags = 'tags'
    users = 'users'


class Aircall2DataSource(ToucanDataSource):
    """
    The Aircall datasource model. Contains:

    - limit (number of entries with 1 being the default)
    - the dataset (a required enum of three possible values, calls, tags or users, default is calls)
    """

    limit: int = Field(MAX_RUNS, description='Limit of entries (default is 1 run)', ge=-1)
    dataset: AircallDataset = 'calls'


class Aircall2Connector(ToucanConnector):
    """
    This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    """

    data_source_model: Aircall2DataSource

    _auth_flow = 'oauth2'

    auth_flow_id: Optional[str]

    async def _get_data(
        self, dataset: str, limit, access_token: str
    ) -> Tuple[List[dict], List[dict]]:
        """Trigger fetches for data and does preliminary filtering process."""
        headers = {'Authorization': f'Bearer {access_token}'}
        async with ClientSession(headers=headers) as session:
            team_data, variable_data = await asyncio.gather(
                fetch_page(
                    'teams',
                    [],
                    session,
                    limit,
                    0,
                    access_token,
                ),
                fetch_page(
                    dataset,
                    [],
                    session,
                    limit,
                    0,
                    access_token,
                ),
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

    async def _get_tags(self, dataset: str, limit, access_token: str) -> List[dict]:
        """Trigger fetches for tags and does preliminary filtering process."""
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

    def run_fetches(self, dataset, limit, access_token) -> Tuple[List[dict], List[dict]]:
        """Set up event loop and fetches for 'calls' and 'users' datasets."""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_data(dataset, limit, access_token))
        return loop.run_until_complete(future)

    def run_fetches_for_tags(self, dataset, limit, access_token):
        """Set up event loop and fetches for 'tags' dataset."""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_tags(dataset, limit, access_token))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: Aircall2DataSource, **kwargs) -> pd.DataFrame:
        """Retrieve data from AirCall API."""
        dataset = data_source.dataset
        empty_df = build_empty_df(dataset)

        # NOTE: no check needed on limit here because a non-valid limit
        # raises a Pydantic ValidationError
        limit = data_source.limit

        access_token = retrieve_secrets_from_kwargs(auth_flow_id=self.auth_flow_id, **kwargs)

        if dataset == 'tags':
            non_empty_df = pd.DataFrame([])
            if limit != 0:
                res = self.run_fetches_for_tags(dataset, limit, access_token)
                non_empty_df = pd.DataFrame(res)
            return pd.concat([empty_df, non_empty_df])
        else:
            team_data = pd.DataFrame([])
            variable_data = pd.DataFrame([])
            if limit != 0:
                team_data, variable_data = self.run_fetches(dataset, limit, access_token)
            return build_df(
                dataset, [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)]
            )
