from enum import Enum
from typing import List

import pandas as pd
import pandas_gbq

from toucan_connectors.google_credentials import GoogleCredentials, get_google_oauth2_credentials
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Dialect(str, Enum):
    legacy = 'legacy'
    standard = 'standard'


class GoogleBigQueryDataSource(ToucanDataSource):
    query: str


class GoogleBigQueryConnector(ToucanConnector):
    data_source_model: GoogleBigQueryDataSource

    credentials: GoogleCredentials
    dialect: Dialect = Dialect.legacy
    scopes: List[str] = ["https://www.googleapis.com/auth/bigquery"]

    def _retrieve_data(self, data_source: GoogleBigQueryDataSource) -> pd.DataFrame:
        """
        Uses Pandas read_gbq method to extract data from Big Query into a dataframe
        See: http://pandas.pydata.org/pandas-docs/stable/generated/pandas.io.gbq.read_gbq.html
        Note:
            The parameter reauth is set to True to force Google BigQuery to reauthenticate the user
            for each query. This is necessary when extracting multiple data to avoid the error:
            [Errno 54] Connection reset by peer
        """
        credentials = (get_google_oauth2_credentials(self.credentials)
                       .with_scopes(self.scopes))
        return pandas_gbq.read_gbq(
            query=data_source.query,
            project_id=self.credentials.project_id,
            credentials=credentials,
            dialect=self.dialect
        )
