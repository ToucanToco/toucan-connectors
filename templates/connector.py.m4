include(`templates/cap.m4')
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class cap(TYPE)DataSource(ToucanDataSource):
    query: str


class cap(TYPE)Connector(ToucanConnector):
    data_source_model: cap(TYPE)DataSource

    username: str
    password: str

    def get_df(self, data_source: cap(TYPE)DataSource) -> pd.DataFrame:
        pass
