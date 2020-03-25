from typing import List, Optional, Tuple, Union

import asyncio
from aiohttp import ClientSession
import pandas as pd
from enum import Enum
import pyjq
from pydantic import Field

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

from .helpers import build_df, build_empty_df, generate_multiple_jq_filters, generate_tags_filter

PER_PAGE = 50
STUFF = '156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@'


async def bulk_fetch(
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

    if next_page_link is not None and current_pass != limit:
        new_endpoint = next_page_link
        new_endpoint = new_endpoint.replace('//', f'//{STUFF}')
        data_list = await bulk_fetch(new_endpoint, data_list, session, limit, current_pass)

    return data_list


async def fetch(new_endpoint, session) -> dict:
    async with session.get(new_endpoint) as res:
        return await res.json()


class AircallDataset(str, Enum):
    calls = 'calls'
    tags = 'tags'
    users = 'users'


class AircallDataSource(ToucanDataSource):
    limit: int = Field(100, description='Limit of entries (-1 for no limit)', ge=-1)
    query: Optional[dict] = {}
    dataset: AircallDataset = 'teams'
    BASE_ROUTE = 'https://156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@api.aircall.io/v1/'


class AircallConnector(ToucanConnector):
    """
    This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    using [Bearer.sh](https://app.bearer.sh/)
    """

    data_source_model: AircallDataSource
    bearer_integration = 'aircall_oauth'
    bearer_auth_id: str

    # def _get_data_data(
    #     self, endpoint, query, jq_filter: str, page_number: int, per_page: int
    # ) -> Tuple[List[dict], bool]:
    #     """Get the data for a single page and the information if the page is the last one"""
    #     page_raw_data = self.bearer_oauth_get_endpoint(
    #         endpoint, {**query, 'per_page': per_page, 'page': page_number}
    #     )
    #     try:
    #         is_last_page = page_raw_data['meta']['next_page_link'] is None
    #     except KeyError:
    #         is_last_page = True
    #     page_data = pyjq.first(jq_filter, page_raw_data)
    #     if isinstance(page_data, dict):
    #         page_data = [page_data]
    #     return page_data, is_last_page

    async def _get_data(
        self, dataset: str, query, limit
    ) -> Union[Tuple[List[dict], List[dict]], List[dict]]:
        BASE_ROUTE = f'https://{STUFF}api.aircall.io/v1/'
        variable_endpoint = f'{BASE_ROUTE}/{dataset}?per_page={PER_PAGE}'

        print('async data called')
        # limit = float('inf') if data_source.limit == -1 else data_source.limit
        async with ClientSession() as session:
            if dataset == 'tags':
                raw_data = await bulk_fetch(variable_endpoint, [], session, limit, 1)

                jq_filter = generate_tags_filter(dataset)

                return pyjq.first(jq_filter, {'results' : raw_data})
            else:
                teams_endpoint = f'{BASE_ROUTE}/teams'

                team_data, variable_data = await asyncio.gather(
                    bulk_fetch(teams_endpoint, [], session, limit, 0),
                    bulk_fetch(variable_endpoint, [], session, limit, 0)
                )

                team_jq_filter, variable_jq_filter = generate_multiple_jq_filters(dataset)

                team_data = pyjq.first(team_jq_filter, {'results' : team_data})
                variable_data = pyjq.first(variable_jq_filter, {'results' : variable_data})

                return team_data, variable_data

    def run_fetches(self, dataset, query, limit):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self._get_data(dataset, query, limit))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
        print('retrieve data called')
        # endpoint = nosql_apply_parameters_to_query(data_source.endpoint, data_source.parameters)
        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        limit = float('inf') if data_source.limit == -1 else data_source.limit
        dataset = data_source.dataset
        empty_df = build_empty_df(dataset)

        print('query ', query)
        print('limit ', limit)

        res = self.run_fetches(dataset, query, limit)

        if dataset == 'tags':
            non_empty_df = pd.DataFrame(res)

            # df = pd.concat([empty_df, non_empty_df])
            # print('df ', df)
            return pd.concat([empty_df, non_empty_df])
        else:
            team_data, variable_data = res
            df = build_df(
                dataset,
                [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)]
            )
            print('df ', df)
            return build_df(
                dataset,
                [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)]
            )
