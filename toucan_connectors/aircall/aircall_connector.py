from typing import List, Optional, Tuple

import pandas as pd
import pyjq
from pydantic import Field

from toucan_connectors.common import FilterSchema, nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

PER_PAGE = 50


class AircallDataSource(ToucanDataSource):
    endpoint: str = Field(
        ...,
        title='Endpoint of the Aircall API',
        description='See https://developer.aircall.io/api-references/#endpoints',
    )
    filter: str = FilterSchema
    limit: int = Field(100, description='Limit of entries (-1 for no limit)', ge=-1)
    query: Optional[dict] = {}


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

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
        endpoint = nosql_apply_parameters_to_query(data_source.endpoint, data_source.parameters)
        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        limit = float('inf') if data_source.limit == -1 else data_source.limit

        current_page = 1
        is_last_page = False
        data = []

        while limit > 0 and not is_last_page:
            per_page = PER_PAGE if limit > PER_PAGE else limit

            # data = [], current_page = 1, limit = 60
            page_data, is_last_page = self._get_page_data(
                endpoint, query, data_source.filter, current_page, per_page
            )

            # data = [{...}, ..., {...}], current_page = 2, limit = 10
            data += page_data
            current_page += 1
            limit -= per_page

        return pd.DataFrame(data)
