from enum import Enum

import pandas as pd
from toucan_client import ToucanClient

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Endpoints(str, Enum):
    users = 'users'
    small_apps = 'small-apps'


class ToucanTocoDataSource(ToucanDataSource):
    endpoint: Endpoints


class ToucanTocoConnector(ToucanConnector):
    type = "ToucanToco"
    data_source_model: ToucanTocoDataSource

    host: str
    username: str
    password: str

    def get_df(self, data_source: ToucanTocoDataSource) -> pd.DataFrame:
        tc = ToucanClient(self.host, auth=(self.username, self.password))
        return pd.DataFrame(tc[data_source.endpoint].get().json())
