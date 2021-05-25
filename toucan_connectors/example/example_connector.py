import pandas as pd

from toucan_connectors.toucan_connector import (
    ToucanConnector,
    ToucanDataSource,
)


class ExampleDataSource(ToucanDataSource):
    """Default Class"""
    query: str


class ExampleConnector(ToucanConnector):
    """Model of my connector"""
    data_source_model: ExampleDataSource

    def _retrieve_data(self, data_source: ExampleDataSource) -> pd.DataFrame:
        """how to retrieve a dataframe"""

