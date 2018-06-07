import pandas as pd
from typing import List

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from adobe_analytics import Client, ReportDefinition


class AdobeAnalyticsDataSource(ToucanDataSource):
    suite_id: str
    dimensions: List[str]
    metrics: List[str]
    date_from: str
    date_to: str

    @property
    def report_definition(self):
        return ReportDefinition(
                dimensions=self.dimensions,
                metrics=self.metrics,
                date_from=self.date_from,
                date_to=self.date_to
            )


class AdobeAnalyticsConnector(ToucanConnector):
    type = "AdobeAnalytics"
    data_source_model: AdobeAnalyticsDataSource

    username: str
    password: str
    endpoint: str = Client.DEFAULT_ENDPOINT

    def get_df(self, data_source: AdobeAnalyticsDataSource) -> pd.DataFrame:
        client = Client(self.username, self.password, self.endpoint)
        suites = client.suites()
        suite = suites[data_source.suite_id]
        dataframe = suite.download(data_source.report_definition)
        return dataframe
