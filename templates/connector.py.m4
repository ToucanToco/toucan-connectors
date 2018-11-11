include(`templates/cap.m4')
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class cap(type)DataSource(ToucanDataSource):
    query: str


class cap(type)Connector(ToucanConnector):
    type = "type"
    data_source_model: cap(type)DataSource

    username: str
    password: str

    def get_df(self, data_source: cap(type)DataSource) -> pd.DataFrame:
        pass
