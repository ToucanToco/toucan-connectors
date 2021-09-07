import logging
from enum import Enum
from timeit import default_timer as timer
from typing import Dict, List, Optional

import pandas
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from pydantic import Field

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


# TODO - check bad return type
def _define_type(value) -> str:
    if isinstance(value, bool):
        return 'BOOL'
    elif isinstance(value, int):
        return 'NUMERIC'
    elif isinstance(value, float):
        return 'FLOAT64'
    elif isinstance(value, str):
        return 'STRING'
    else:
        return 'STRING'


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

    @staticmethod
    def _get_google_credentials(credentials: GoogleCredentials, scopes: List[str]) -> Credentials:
        credentials = get_google_oauth2_credentials(credentials).with_scopes(scopes)
        return credentials

    @staticmethod
    def _connect(credentials: Credentials) -> bigquery.Client:
        start = timer()
        client = bigquery.Client(credentials=credentials)
        end = timer()
        logging.getLogger(__name__).info(
            f'[benchmark][google_big_query] - connect {end - start} seconds',
            extra={
                'benchmark': {
                    'operation': 'connect',
                    'execution_time': end - start,
                    'connector': 'google_big_query',
                }
            },
        )
        return client

    @staticmethod
    def _execute_query(client: bigquery.Client, query: str, parameters: List) -> pandas.DataFrame:
        try:
            start = timer()
            result = (
                client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=parameters))
                .result()
                .to_dataframe(
                    create_bqstorage_client=True,
                )  # Use to generate directly a dataframe pandas
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
        except TypeError as e:
            logging.getLogger(__name__).error(f'Error to execute request {query} - {e}')
            raise e

    @staticmethod
    def _prepare_parameters(query: str, parameters: Optional[Dict]) -> List:
        query_parameters = []
        """replace ToucanToco variable definition by Google Big Query variable definition"""
        for k in parameters or {}:
            if query.find('@' + k) > -1:
                # set all parameters with a type defined and necessary for Big Query
                query_parameters.append(
                    bigquery.ScalarQueryParameter(k, _define_type(parameters[k]), parameters[k])
                )
        return query_parameters

    @staticmethod
    def _prepare_query(query: str) -> str:
        """replace ToucanToco variable definition by Google Big Query variable definition"""
        new_query = query.replace('{{', '@').replace('}}', '')
        return new_query

    def _retrieve_data(self, data_source: GoogleBigQueryDataSource) -> pd.DataFrame:
        logging.getLogger(__name__).debug(
            f'Play request {data_source.query} with parameters {data_source.parameters}'
        )

        credentials = GoogleBigQueryConnector._get_google_credentials(self.credentials, self.scopes)
        query = GoogleBigQueryConnector._prepare_query(data_source.query)
        parameters = GoogleBigQueryConnector._prepare_parameters(query, data_source.parameters)

        client = GoogleBigQueryConnector._connect(credentials)
        result = GoogleBigQueryConnector._execute_query(client, query, parameters)

        return result
