from enum import Enum

import pandas as pd
from typing import List, Union

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from adobe_analytics import Client, ReportDefinition


class Granularity(str, Enum):
    hour = "hour"
    day = "day"
    week = "week"
    month = "month"
    quarter = "quarter"
    year = "year"


class AdobeAnalyticsDataSource(ToucanDataSource):
    suite_id: str

    dimensions: Union[List[Union[str, dict]], str] = []
    metrics: Union[List[str], str]
    segments: Union[List[str], str] = None

    date_from: str
    date_to: str
    last_days: int = None
    granularity: Granularity = None
    source: str = None

    @property
    def report_definition(self):
        return ReportDefinition(
            segments=self.segments,
            dimensions=self.dimensions,
            metrics=self.metrics,
            date_from=self.date_from,
            date_to=self.date_to,
            last_days=self.last_days,
            granularity=self.granularity,
            source=self.source
        )


class AdobeAnalyticsConnector(ToucanConnector):
    """
    Adobe Analytics Connector using Adobe Analytics' REST API v1.4.
    It provides a high-level interfaces for reporting queries (including Data Warehouse requests).
    """
    type = "AdobeAnalytics"
    data_source_model: AdobeAnalyticsDataSource

    username: str
    password: str
    endpoint: str = Client.DEFAULT_ENDPOINT

    def get_df(self, data_source: AdobeAnalyticsDataSource) -> pd.DataFrame:
        suites = Client(self.username, self.password, self.endpoint).suites()
        df = suites[data_source.suite_id].download(data_source.report_definition)
        df['suite_id'] = data_source.suite_id
        return df
