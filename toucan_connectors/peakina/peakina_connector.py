from logging import getLogger
from typing import Any

from pydantic import ConfigDict

from toucan_connectors.toucan_connector import ToucanConnector

try:
    import pandas as pd
    from peakina.datasource import DataSource

    class PeakinaDataSource(DataSource):
        model_config = ConfigDict(extra="allow")

        def __init__(self, **data: Any) -> None:
            super().__init__(**data)

    class PeakinaConnector(ToucanConnector, data_source_model=PeakinaDataSource):
        def _retrieve_data(self, data_source: PeakinaDataSource) -> pd.DataFrame:
            return data_source.get_df()

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False
