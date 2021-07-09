import logging
from enum import Enum
from timeit import default_timer as timer
from typing import List

import pandas as pd
import pandas_gbq
from pydantic import Field

from toucan_connectors.common import apply_query_parameters
from toucan_connectors.google_credentials import GoogleCredentials, get_google_oauth2_credentials
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Dialect(str, Enum):
    legacy = 'legacy'
    standard = 'standard'


class GoogleBigQueryDataSource(ToucanDataSource):
    query: str = Field(
        ...,
        description='You can find details on the query syntax '
        '<a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax">here</a>',
        widget='sql',
    )


class GoogleBigQueryConnector(ToucanConnector):
    data_source_model: GoogleBigQueryDataSource

    credentials: GoogleCredentials = Field(
        ...,
        title='Google Credentials',
        description='For authentication, download an authentication file from your '
        '<a href="https://console.developers.google.com/apis/credentials" target="_blank">Google Console</a> and '
        'use the values here. This is an oauth2 credential file. For more information see this '
        '<a href="https://gspread.readthedocs.io/en/latest/oauth2.html" target="_blank" >documentation</a>. '
        'You should use "service_account" credentials, which is the preferred type of credentials '
        'to use when authenticating on behalf of a service or application',
    )
    dialect: Dialect = Field(
        Dialect.standard,
        description='BigQuery allows you to choose between standard and legacy SQL as query syntax. '
        'The preferred query syntax is the default standard SQL. You can find more information on this '
        '<a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax" target="_blank" >documentation</a>',
    )
    scopes: List[str] = Field(
        ['https://www.googleapis.com/auth/bigquery'],
        title='OAuth scopes',
        description='OAuth 2.0 scopes define the level of access you need to request '
        'the Google APIs. For more information, see this '
        '<a href="https://developers.google.com/identity/protocols/googlescopes" target="_blank" >documentation</a>',
    )

    def _retrieve_data(self, data_source: GoogleBigQueryDataSource) -> pd.DataFrame:
        """
        Uses Pandas read_gbq method to extract data from Big Query into a dataframe
        See: http://pandas.pydata.org/pandas-docs/stable/generated/pandas.io.gbq.read_gbq.html
        Note:
            The parameter reauth is set to True to force Google BigQuery to reauthenticate the user
            for each query. This is necessary when extracting multiple data to avoid the error:
            [Errno 54] Connection reset by peer
        """
        start = timer()
        data_source.query = apply_query_parameters(data_source.query, data_source.parameters)
        logging.getLogger(__name__).debug(f'Play request {data_source.query}')
        credentials = get_google_oauth2_credentials(self.credentials).with_scopes(self.scopes)
        result = pandas_gbq.read_gbq(
            query=data_source.query,
            project_id=self.credentials.project_id,
            credentials=credentials,
            dialect=self.dialect,
        )
        end = timer()
        logging.getLogger(__name__).info(
            f'[benchmark][google_big_query] - execute {end - start} seconds',
            extra={
                'benchmark': {
                    'operation': 'execute',
                    'execution_time': end - start,
                    'connector': 'google_big_query',
                }
            },
        )
        return result
