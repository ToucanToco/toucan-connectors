import asyncio
import os
from enum import Enum
from typing import List, Optional, Tuple

import pandas as pd
from aiohttp import ClientSession
from pydantic import Field

from toucan_connectors.common import get_loop
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

from .constants import MAX_RUNS, PER_PAGE
from .helpers import DICTIONARY_OF_FORMATTERS, build_df, build_empty_df

BASE_ROUTE = 'https://proxy.bearer.sh/aircall_oauth'
BEARER_API_KEY = os.environ.get('BEARER_API_KEY')


async def fetch_page(
    dataset: str,
    data_list: List[dict],
    session: ClientSession,
    limit,
    current_pass: int,
    new_page=1,
) -> List[dict]:
    """
    Fetches data from AirCall API

    dependent on existence of other pages and call limit
    """
    endpoint = f'{BASE_ROUTE}/{dataset}?per_page={PER_PAGE}&page={new_page}'
    data: dict = await fetch(endpoint, session)

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


class AircallDataset(str, Enum):
    calls = 'calls'
    tags = 'tags'
    users = 'users'


class AircallDataSource(ToucanDataSource):
    limit: int = Field(MAX_RUNS, description='Limit of entries (default is 1 run)', ge=-1)
    dataset: AircallDataset = 'calls'


class AircallConnector(ToucanConnector):
    """
    This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    using [Bearer.sh](https://app.bearer.sh/)
    """

    data_source_model: AircallDataSource
    bearer_integration = 'aircall_oauth'
    bearer_auth_id: str

    async def _get_data(self, dataset: str, limit) -> Tuple[List[dict], List[dict]]:
        """Triggers fetches for data and does preliminary filtering process"""
        headers = {'Authorization': BEARER_API_KEY, 'Bearer-Auth-Id': self.bearer_auth_id}
        async with ClientSession(headers=headers) as session:
            team_data, variable_data = await asyncio.gather(
                fetch_page('teams', [], session, limit, 0,),
                fetch_page(dataset, [], session, limit, 0,),
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
        headers = {'Authorization': BEARER_API_KEY, 'Bearer-Auth-Id': self.bearer_auth_id}
        async with ClientSession(headers=headers) as session:
            raw_data = await fetch_page(dataset, [], session, limit, 1,)
            tags_data_list = []
            for data in raw_data:
                tags_data_list += data['tags']
            return tags_data_list

    def run_fetches(self, dataset, limit) -> Tuple[List[dict], List[dict]]:
        """sets up event loop and fetches for 'calls' and 'users' datasets"""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_data(dataset, limit))
        return loop.run_until_complete(future)

    def run_fetches_for_tags(self, dataset, limit):
        """sets up event loop and fetches for 'tags' dataset"""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_tags(dataset, limit))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
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
                team_data, variable_data = self.run_fetches(dataset, limit)
            return build_df(
                dataset, [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)]
            )
