import json
from enum import Enum

import pandas as pd
import pandas_gbq

from toucan_connectors.common import GoogleCredentials
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Dialect(str, Enum):
    legacy = 'legacy'
    standard = 'standard'


class GoogleBigQueryDataSource(ToucanDataSource):
    query: str


class GoogleBigQueryConnector(ToucanConnector):
    type = "GoogleBigQuery"
    data_source_model: GoogleBigQueryDataSource

    credentials: GoogleCredentials
    dialect: Dialect = Dialect.legacy

    def get_df(self, data_source: GoogleBigQueryDataSource) -> pd.DataFrame:
        """
        Uses Pandas read_gbq method to extract data from Big Query into a dataframe
        See: http://pandas.pydata.org/pandas-docs/stable/generated/pandas.io.gbq.read_gbq.html
        Note:
            The parameter reauth is set to True to force Google BigQuery to reauthenticate the user
            for each query. This is necessary when extracting multiple data to avoid the error:
            [Errno 54] Connection reset by peer
        """
        return pandas_gbq.read_gbq(
            query=data_source.query,
            project_id=self.credentials.project_id,
            private_key=json.dumps(self.credentials.dict()),
            reauth=True,
            dialect=self.dialect
        )
