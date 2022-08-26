import logging
from enum import Enum
from functools import cached_property
from timeit import default_timer as timer
from typing import Any, Dict, List, Optional, Union

import pandas
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.dbapi import _helpers as bigquery_helpers
from google.oauth2.service_account import Credentials
from pydantic import Field, create_model

from toucan_connectors.google_credentials import GoogleCredentials, get_google_oauth2_credentials
from toucan_connectors.toucan_connector import (
    DiscoverableConnector,
    TableInfo,
    ToucanConnector,
    ToucanDataSource,
)


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
    query_object: Dict = Field(
        None,
        description='An object describing a simple select query This field is used internally',
        **{'ui.hidden': True},
    )
    language: str = Field('sql', **{'ui.hidden': True})
    database: str = Field(None)  # Needed for graphical selection in frontend but not used

    class Config:
        extra = 'ignore'

    @classmethod
    def get_form(cls, connector: 'GoogleBigQueryConnector', current_config: dict[str, Any]):
        schema = create_model('FormSchema', __base__=cls).schema()
        schema['properties']['database']['default'] = connector.credentials.project_id
        return schema


BigQueryParam = Union[bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter]


def _define_query_param(name: str, value: Any) -> BigQueryParam:
    if isinstance(value, list):
        return (
            bigquery_helpers.array_to_query_parameter(value=value, name=name)
            if len(value) > 0
            # array_to_query_parameter raises an exception in case of an empty list
            else bigquery.ArrayQueryParameter(name=name, array_type='STRING', values=value)
        )
    else:
        return bigquery_helpers.scalar_to_query_parameter(value=value, name=name)


class GoogleBigQueryConnector(ToucanConnector, DiscoverableConnector):
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

    class Config:
        underscore_attrs_are_private = True
        keep_untouched = (cached_property,)

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

    @cached_property
    def project_tree(self) -> list[TableInfo]:
        return self._get_project_structure()

    @staticmethod
    def _execute_query(client: bigquery.Client, query: str, parameters: List) -> pandas.DataFrame:
        try:
            start = timer()

            result = (
                client.query(
                    query.replace(
                        '"', '`'
                    ),  # Ugly but the select query generated by frontend adds '"'
                    job_config=bigquery.QueryJobConfig(query_parameters=parameters),
                )
                .result()
                .to_dataframe(
                    create_bqstorage_client=True,
                    date_as_object=False,  # so date columns get dtype 'datetime64[ns]' instead of 'object'
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
        """replace ToucanToco variable definitions by Google Big Query variable definition"""
        query_parameters = []
        for param_name, param_value in (parameters or {}).items():
            if query.find('@' + param_name) > -1:
                # set all parameters with a type defined and necessary for Big Query
                query_parameters.append(_define_query_param(param_name, param_value))
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

    @classmethod
    def _format_db_model(cls, unformatted_db_tree: pd.DataFrame) -> List[TableInfo]:
        def _format_columns(x: str):
            col = x.split()
            return {'name': col[0], 'type': col[1]}

        unformatted_db_tree['type'] = unformatted_db_tree['type'].apply(
            lambda x: 'view' if 'VIEW' in x else 'table'
        )
        unformatted_db_tree['columns'] = (
            unformatted_db_tree['column_name']
            + ' '
            + unformatted_db_tree['data_type'].apply(lambda x: x.lower())
        )

        unformatted_db_tree['columns'] = unformatted_db_tree['columns'].apply(_format_columns)
        return (
            unformatted_db_tree.groupby(['name', 'schema', 'database', 'type'])['columns']
            .apply(list)
            .reset_index()
            .to_dict(orient='records')
        )

    def _get_project_structure(self) -> List[TableInfo]:
        client = self._connect(
            GoogleBigQueryConnector._get_google_credentials(self.credentials, self.scopes)
        )
        datasets = list(client.list_datasets())
        query = '\nUNION ALL\n'.join(
            [
                f"""select C.table_name as name, C.table_schema as schema, T.table_catalog as database,
                T.table_type as type,  C.column_name, C.data_type from {dataset.dataset_id}.INFORMATION_SCHEMA.COLUMNS C
                JOIN {dataset.dataset_id}.INFORMATION_SCHEMA.TABLES T on C.table_name = T.table_name
                where IS_SYSTEM_DEFINED='NO' AND IS_PARTITIONING_COLUMN='NO' AND IS_HIDDEN='NO'"""
                for dataset in datasets
            ]
        )
        return self._format_db_model(client.query(query).to_dataframe())

    def get_model(self, db_name: str | None = None) -> list[TableInfo]:
        """Retrieves the database tree structure using current connection"""
        return self.project_tree
