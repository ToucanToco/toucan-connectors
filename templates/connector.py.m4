include(`templates/cap.m4')
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class cap(name)DataSource(ToucanDataSource):
    query: str


class cap(name)Connector(ToucanConnector):
    type = "name"
    data_source_model: cap(name)DataSource

    username: str
    password: str

    def get_df(self, data_source: cap(name)DataSource) -> pd.DataFrame:
        pass
