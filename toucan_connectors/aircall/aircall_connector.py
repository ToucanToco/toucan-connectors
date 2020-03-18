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

from .helpers import build_full_user_list, reshape_users_in_calls

PER_PAGE = 50


async def fetch(new_endpoint, session):
    async with session.get(new_endpoint) as res:
        # print(res.status)
        return await res.json()


class AircallRoutes(str, Enum):
    calls = "calls"
    tags = "tags"
    teams = "teams"


class AircallDataSource(ToucanDataSource):
    endpoint: str = Field(
        ...,
        title='Endpoint of the Aircall API',
        description='See https://developer.aircall.io/api-references/#endpoints',
    )
    filter: str = FilterSchema
    limit: int = Field(100, description='Limit of entries (-1 for no limit)', ge=-1)
    query: Optional[dict] = {}
    partial_urls: List[str] = [partial_url for partial_url in AircallRoutes]
    BASE_ROUTE = "https://156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@api.aircall.io/v1/"


class AircallConnector(ToucanConnector):
    """
    This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    using [Bearer.sh](https://app.bearer.sh/)
    """

    data_source_model: AircallDataSource
    bearer_integration = 'aircall_oauth'
    bearer_auth_id: str

    # build a generic route with the "/calls" route as default
    def build_aircall_url(self, BASE_ROUTE: str, partial_urls, route: str = "") -> str:
        url: str = f'{BASE_ROUTE}'

        if route:
            if route in partial_urls:
                url += route
            else:
                err_str = "This is not a valid Aircall route"
                print(err_str)
                raise ValueError(err_str)
        else:
            url += "calls"

        return url

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
        self, endpoint, query, jq_filter: str, page_number: int, per_page: int
    ):
        BASE_ROUTE = "https://156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@api.aircall.io/v1/"
        partial_urls: List[str] = [partial_url for partial_url in AircallRoutes]
        print("async data called")
        print("jq filter ", jq_filter)
        request_param: str = endpoint.strip("/")
        new_endpoint = self.build_aircall_url(BASE_ROUTE, partial_urls, request_param)
        # limit = float('inf') if data_source.limit == -1 else data_source.limit
        async with ClientSession() as session:
            if endpoint == "/teams":
                users_endpoint = f'{BASE_ROUTE}/users'
                team_data, users_data = await asyncio.gather(fetch(new_endpoint, session), fetch(users_endpoint, session))
                teams = team_data.get("teams", [])
                users = users_data.get("users", [])
                pool_of_users = {'teams' : build_full_user_list(users, teams)}
                # pool_of_users = build_full_user_list(users, teams)

                # print("pool of users ", pool_of_users)
                test = jq(jq_filter).transform(pool_of_users)
                # print("test ", test)
                column_names = {
                    "id": "user_id",
                    "created_at": "user_created_at",
                    "name" : "user_name"
                }
                new_pd = pd.DataFrame(test)
                print(new_pd)
                return (
                    new_pd
                    .rename(columns=column_names)
                    .reindex(columns=["team", "user_id", "user_name", "user_created_at"])
                    .assign(**{"user_created_at": lambda x: x["user_created_at"].str[:10]})
                )
            else:
                raw_data = await fetch(new_endpoint, session)
                # print(raw_data)
                print(raw_data["calls"][8])
                data = jq(jq_filter).transform(raw_data)
                # print(data[8])
                # new_pd = pd.DataFrame(data)
                # print(new_pd)

                if request_param == "calls":
                    test = reshape_users_in_calls(data)
                    test = json_normalize(test, meta=[["user", "id"], ["user", "name"]]).rename(columns={"user.id": "user_id", "user.name": "user_name"})
                    test = test.where(test.notnull(), None)
                    # test = test.assign(**{
                    #     "answered_at" : lambda t: pd.to_datetime(t["answered_at"], unit="s"),
                    #     "ended_at": lambda t: pd.to_datetime(t['ended_at'], unit="s"),
                    #     "day": lambda t: t["ended_at"].astype(str).str[:10]})
                    # print(test[:9])
        # return res

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
        print('retrieve data called')
        endpoint = nosql_apply_parameters_to_query(data_source.endpoint, data_source.parameters)
        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        limit = float('inf') if data_source.limit == -1 else data_source.limit

        current_page = 1
        is_last_page = False
        data = []

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._get_page_data_async(
            endpoint, query, data_source.filter, current_page, 1
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
