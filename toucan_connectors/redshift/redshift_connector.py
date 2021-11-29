
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class RedshiftDataSource(ToucanDataSource):
    query: str


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource

    username: str
    password: str

    def _retrieve_data(self, data_source: RedshiftDataSource) -> pd.DataFrame:
        pass
