from typing import List, Optional, Tuple

import asyncio
from aiohttp import ClientSession
import pandas as pd
from pandas.io.json import json_normalize
from enum import Enum
from jq import jq
from pydantic import Field

from toucan_connectors.common import FilterSchema, nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

from .helpers import build_empty_df, generate_users_jq_filters, generate_single_jq_filters

PER_PAGE = 50


async def fetch(new_endpoint, session):
    async with session.get(new_endpoint) as res:
        # print(res.status)
        return await res.json()


class AircallDataset(str, Enum):
    calls = 'calls'
    tags = 'tags'
    users = 'users'


class AircallDataSource(ToucanDataSource):
    # endpoint: str = Field(
    #     ...,
    #     title='Endpoint of the Aircall API',
    #     description='See https://developer.aircall.io/api-references/#endpoints',
    # )
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
        page_data = jq(jq_filter).transform(page_raw_data)
        if isinstance(page_data, dict):
            page_data = [page_data]
        return page_data, is_last_page

    async def _get_page_data_async(
        self, dataset, query, page_number: int, per_page: int
    ):
        BASE_ROUTE = 'https://156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@api.aircall.io/v1/'
        print('async data called')
        # limit = float('inf') if data_source.limit == -1 else data_source.limit
        async with ClientSession() as session:
            print('dataset ', dataset)
            if dataset == 'users':
                teams_endpoint = f'{BASE_ROUTE}/teams'
                users_endpoint = f'{BASE_ROUTE}/users'

                team_data, users_data = await asyncio.gather(fetch(teams_endpoint, session), fetch(users_endpoint, session))

                teams_jq_filter, users_jq_filter = generate_users_jq_filters(dataset)

                team_data = jq(teams_jq_filter).transform(team_data)
                users_data = jq(users_jq_filter).transform(users_data)

                df_empty = pd.DataFrame(columns=['user_id', 'user_name', 'team', 'user_created_at'])
                df_teams = pd.DataFrame(team_data)
                df_users = pd.DataFrame(users_data)

                df = (pd
                      .concat([df_empty, df_teams, df_users], sort=False, ignore_index=True)
                      .drop_duplicates(['user_id'], keep='first')
                      .assign(**{'user_created_at': lambda x: x['user_created_at'].str[:10]})
                      )

                print(df)
            else:
                endpoint = f'{BASE_ROUTE}/{dataset}'
                # raw_data = await fetch(endpoint, session)
                raw_data = { 'tags': []}
                # print(raw_data)
                # print(raw_data['calls'][8:10])
                jq_filter = generate_single_jq_filters(dataset)
                data = jq(jq_filter).transform(raw_data)
                # print(data[8])
                empty_df = build_empty_df(dataset)
                non_empty_df = pd.DataFrame(data)
                new_df = pd.concat([empty_df, non_empty_df])
                print(new_df)

                # if request_param == 'calls':
                #     test = reshape_users_in_calls(data)
                #     test = json_normalize(test, meta=[['user', 'id'], ['user', 'name']]).rename(columns={'user.id': 'user_id', 'user.name': 'user_name'})
                #     test = test.where(test.notNone(), None)
                    # test = test.assign(**{
                    #     'answered_at' : lambda t: pd.to_datetime(t['answered_at'], unit='s'),
                    #     'ended_at': lambda t: pd.to_datetime(t['ended_at'], unit='s'),
                    #     'day': lambda t: t['ended_at'].astype(str).str[:10]})
                    # print(test[:9])
        # return res

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
        print('retrieve data called')
        # endpoint = nosql_apply_parameters_to_query(data_source.endpoint, data_source.parameters)
        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        limit = float('inf') if data_source.limit == -1 else data_source.limit

        current_page = 1
        is_last_page = False
        data = []

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._get_page_data_async(
            data_source.dataset, query, current_page, 1
        ))
        loop.close()

        # while limit > 0 and not is_last_page:
        #     per_page = PER_PAGE if limit > PER_PAGE else limit

        #     # data = [], current_page = 1, limit = 60
        #     page_data, is_last_page = self._get_page_data(
        #         endpoint, query, data_source.filter, current_page, per_page
        #     )
        #     # data = [{...}, ..., {...}], current_page = 2, limit = 10
        #     data += page_data
        #     current_page += 1
        #     limit -= per_page

        return pd.DataFrame(data)
