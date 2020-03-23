from typing import List, Optional, Tuple

import asyncio
from aiohttp import ClientSession
import pandas as pd
from enum import Enum
import pyjq
from pydantic import Field

from toucan_connectors.common import FilterSchema, nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

from .helpers import build_df, build_empty_df, generate_multiple_jq_filters

PER_PAGE = 50


async def fetch(new_endpoint, session):
    async with session.get(new_endpoint) as res:
        return await res.json()


class AircallDataset(str, Enum):
    calls = 'calls'
    tags = 'tags'
    users = 'users'


class AircallDataSource(ToucanDataSource):
    # filter: str = FilterSchema
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

    def _get_page_data(
        self, endpoint, query, jq_filter: str, page_number: int, per_page: int
    ) -> Tuple[List[dict], bool]:
        """Get the data for a single page and the information if the page is the last one"""
        page_raw_data = self.bearer_oauth_get_endpoint(
            endpoint, {**query, 'per_page': per_page, 'page': page_number}
        )
        try:
            is_last_page = page_raw_data['meta']['next_page_link'] is None
        except KeyError:
            is_last_page = True
        page_data = pyjq.first(jq_filter, page_raw_data)
        if isinstance(page_data, dict):
            page_data = [page_data]
        return page_data, is_last_page

    async def _get_page_data_async(
        self, dataset: str, query, page_number: int, per_page: int
    ):
        BASE_ROUTE = 'https://156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@api.aircall.io/v1/'
        print('async data called')
        # limit = float('inf') if data_source.limit == -1 else data_source.limit
        async with ClientSession() as session:
            if dataset == 'tags':
                endpoint = f'{BASE_ROUTE}/{dataset}'

                raw_data = await fetch(endpoint, session)

                jq_filter = f'.{dataset}'

                return pyjq.first(jq_filter, raw_data)
            else:
                teams_endpoint = f'{BASE_ROUTE}/teams'
                variable_endpoint = f'{BASE_ROUTE}/{dataset}'

                team_data, variable_data = await asyncio.gather(fetch(teams_endpoint, session), fetch(variable_endpoint, session))

                team_jq_filter, variable_jq_filter = generate_multiple_jq_filters(dataset)

                team_data = pyjq.first(team_jq_filter, team_data)
                variable_data = pyjq.first(variable_jq_filter, variable_data)

                return team_data, variable_data

    def run_fetches(self, datasource, query, current_page, last_page):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self._get_page_data_async(datasource, query, current_page, last_page))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
        print('retrieve data called')
        # endpoint = nosql_apply_parameters_to_query(data_source.endpoint, data_source.parameters)
        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        limit = float('inf') if data_source.limit == -1 else data_source.limit
        dataset = data_source.dataset
        empty_df = build_empty_df(dataset)

        current_page = 1
        is_last_page = False
        data = []

        res = self.run_fetches(dataset, query, current_page, 1)

        if dataset == 'tags':
            non_empty_df = pd.DataFrame(res)

            # df = pd.concat([empty_df, non_empty_df])
            # print('df ', df)
            return pd.concat([empty_df, non_empty_df])
        else:
            team_data, variable_data = res
            # df = build_df(dataset, [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)])
            # print('df ', df)
            return build_df(dataset, [empty_df, pd.DataFrame(team_data), pd.DataFrame(variable_data)])

        # while limit > 0 and not is_last_page:
        #     per_page = PER_PAGE if limit > PER_PAGE else limit

        #     # data = [], current_page = 1, limit = 60
        #     # page_data, is_last_page = self._get_page_data(
        #     #     endpoint, query, data_source.filter, current_page, per_page
        #     # )
        #     # data = [{...}, ..., {...}], current_page = 2, limit = 10
        #     data += page_data
        #     current_page += 1
        #     limit -= per_page

        return pd.DataFrame(data)
