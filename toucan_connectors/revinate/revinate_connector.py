"""
Revinate connector

Documentation can be found at: https://porter.revinate.com/documentation

This connector inherits from the HTTP connector classes rather than the base toucan-connector classes
"""
import pandas as pd

from toucan_connectors.http_api.http_api_connector import HttpAPIConnector, HttpAPIDataSource


class RevinateDataSource(HttpAPIDataSource):
    base_url = 'https://porter.revinate.com'


class RevinateConnector(HttpAPIConnector):
    data_source_model: RevinateDataSource

    username: str
    password: str

    def _retrieve_data(self, data_source: RevinateDataSource) -> pd.DataFrame:
        pass
