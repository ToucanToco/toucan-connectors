from enum import Enum

import pandas as pd
from pandas.io.json import json_normalize
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

from .client import Client
from .data import (
    get_attr_names, get_metric_names, flatten_json, get_definition,
    fill_viewfilter_with_ids
)


class Dataset(str, Enum):
    cube = 'cube'
    report = 'report'


class MicroStrategyDataSource(ToucanDataSource):
    """
    Specify whether you want to use the `cube` or `reports` endpoints and a microstrategy doc id.
    """
    id: str
    dataset: Dataset
    viewfilter: dict = None
    offset: int = 0
    limit: int = 100


class MicroStrategyConnector(ToucanConnector):
    """
    Import data from MicroStrategy using the [JSON Data API](http://bit.ly/2HCzf04) for cubes and
    reports.
    """
    data_source_model: MicroStrategyDataSource

    base_url: str
    username: str
    password: str
    project_id: str

    def _retrieve_data(self, data_source: MicroStrategyDataSource) -> pd.DataFrame:
        """Retrieves cube or report data, flattens return dataframe"""
        c = Client(self.base_url, self.project_id, self.username, self.password)

        query_func = getattr(c, data_source.dataset)
        if not data_source.viewfilter:
            results = query_func(
                id=data_source.id,
                offset=data_source.offset,
                limit=data_source.limit,
            )
        else:
            results = query_func(id=data_source.id, limit=0)
            dfn = get_definition(results)
            viewfilter = fill_viewfilter_with_ids(data_source.viewfilter, dfn)
            results = query_func(
                id=data_source.id,
                viewfilter=viewfilter,
                offset=data_source.offset,
                limit=data_source.limit,
            )

        # Get a list of attributes and metrics
        attributes = get_attr_names(results)
        metrics = get_metric_names(results)

        # get data based on attributes and metrics
        rows = flatten_json(results['result']['data']['root'], attributes, metrics)
        return json_normalize(rows)
