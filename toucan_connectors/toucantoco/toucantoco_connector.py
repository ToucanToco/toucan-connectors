from enum import Enum

import pandas as pd
from toucan_client import ToucanClient

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Endpoints(str, Enum):
    users = 'users'
    small_apps = 'small-apps'
    config = 'config'


class ToucanTocoDataSource(ToucanDataSource):
    endpoint: Endpoints
    all_small_apps: bool = False


def g(o): return o.get().json()


class ToucanTocoConnector(ToucanConnector):
    type = "ToucanToco"
    data_source_model: ToucanTocoDataSource

    host: str
    username: str
    password: str

    def get_df(self, data_source: ToucanTocoDataSource) -> pd.DataFrame:
        tc = ToucanClient(self.host, auth=(self.username, self.password))

        if data_source.all_small_apps:
            ret = []
            for app in g(tc['small-apps']):
                ret.append({'small_app': app['id'],
                            'response': g(tc[app['id']][data_source.endpoint])})
            return pd.DataFrame(ret)

        else:
            return pd.DataFrame(g(tc[data_source.endpoint]))
