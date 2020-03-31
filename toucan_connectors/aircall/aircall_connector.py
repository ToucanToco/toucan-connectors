from typing import List, Optional, Tuple, Union

import asyncio
from aiohttp import ClientSession
import pandas as pd
from enum import Enum
import pyjq
from pydantic import Field

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

from .constants import PER_PAGE, MAX_RUNS
from .helpers import build_df, build_empty_df, generate_multiple_jq_filters, generate_tags_filter

# temporary constant that will be removed in production-ready code
STUFF = '156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@'


async def fetch_page(
    base_endpoint: str, data_list: List[dict], session, limit, current_pass: int
) -> List[dict]:
    """
    Fetches data from AirCall API
    dependent on existence of other pages and call limit
    """
    data: dict = await fetch(base_endpoint, session)

    data_list.append(data)

    next_page_link: Optional[str] = data['meta'].get('next_page_link', None)

    current_pass += 1

    if next_page_link is not None and current_pass < limit:
        new_endpoint = next_page_link
        new_endpoint = new_endpoint.replace('//', f'//{STUFF}')
        data_list = await fetch_page(new_endpoint, data_list, session, limit, current_pass)

    return data_list


async def fetch(new_endpoint, session) -> dict:
    async with session.get(new_endpoint) as res:
        return await res.json()


class AircallDataset(str, Enum):
    calls = 'calls'
    tags = 'tags'
    users = 'users'


class AircallDataSource(ToucanDataSource):
    # limit: int = Field(100, description='Limit of entries (-1 for no limit)', ge=-1)
    limit: int = Field(1, description='Limit of entries (default is 1 run)', ge=1)
    query: Optional[dict] = {}
    dataset: AircallDataset = 'users'


class AircallConnector(ToucanConnector):
    """
    This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    using [Bearer.sh](https://app.bearer.sh/)
    """

    data_source_model: AircallDataSource
    bearer_integration = 'aircall_oauth'
    bearer_auth_id: str

    async def _get_data(
        self, dataset: str, query, limit
    ) -> Union[Tuple[List[dict], List[dict]], List[dict]]:
        """Triggers fetches for data and does preliminary filtering process"""
        BASE_ROUTE = f'https://{STUFF}api.aircall.io/v1/'
        variable_endpoint = f'{BASE_ROUTE}/{dataset}?per_page={PER_PAGE}'

        async with ClientSession() as session:
            if dataset == 'tags':
                raw_data = await fetch_page(variable_endpoint, [], session, limit, 1)

                jq_filter = generate_tags_filter(dataset)

                return pyjq.first(jq_filter, {'results' : raw_data})
            else:
                teams_endpoint = f'{BASE_ROUTE}/teams'

                team_data, variable_data = await asyncio.gather(
                    fetch_page(teams_endpoint, [], session, limit, 0),
                    fetch_page(variable_endpoint, [], session, limit, 0)
                )

                team_jq_filter, variable_jq_filter = generate_multiple_jq_filters(dataset)

                team_data = pyjq.first(team_jq_filter, {'results' : team_data})
                variable_data = pyjq.first(variable_jq_filter, {'results' : variable_data})
                return team_data, variable_data

    def run_fetches(self, dataset, query, limit) -> Union[Tuple[List[dict], List[dict]], List[dict]]:
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self._get_data(dataset, query, limit))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        dataset = data_source.dataset
        empty_df = build_empty_df(dataset)

        limit = MAX_RUNS

        if data_source.limit:
            limit = data_source.limit

        res = self.run_fetches(dataset, query, limit)

        if dataset == 'tags':
            non_empty_df = pd.DataFrame(res)
            return pd.concat([empty_df, non_empty_df])
        else:
            team_data, variable_data = res
            return build_df(
                dataset,
                [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)]
            )
