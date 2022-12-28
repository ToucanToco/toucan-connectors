from typing import Any

import pandas as pd
from peakina.datasource import DataSource

from toucan_connectors.toucan_connector import ToucanConnector


class PeakinaDataSource(DataSource):
    class Config:
        extra = 'allow'

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)


class PeakinaConnector(ToucanConnector):
    data_source_model: PeakinaDataSource

    def _retrieve_data(self, data_source: PeakinaDataSource) -> pd.DataFrame:
        return data_source.get_df()
