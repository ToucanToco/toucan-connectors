from contextlib import suppress
from typing import Dict

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr, create_model

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import (
    strlist_to_enum,
    ToucanConnector,
    ToucanDataSource,
)
import boto3

client = boto3.client('redshift')


class RedshiftDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')

    table: str = Field(None, description='The name of the data table that you want to ')

    query: Dict = Field(None, description='An object describing a simple select query')

    @classmethod
    def get_form(cls, connector: 'RedshiftConnector', current_config):
        constraints = {}

        with suppress(Exception):
            connection = redshift_connector.connect(
                **connector.get_connection_params(
                    database=current_config.get('database', 'redshift')
                )
            )
            with connection.cursor() as cursor:
                cursor.execute("""select * from ?""")
                res = cursor.fetchall()
                available_dbs = [db_name for (db_name,) in res]
                constraints['database'] = strlist_to_enum('database', available_dbs)

                if 'database' in current_config:
                    cursor.execute("""select * from ?""")
                    res = cursor.fetchall()
                    available_tables = [table_name for (_, table_name) in res]
                    constraints['table'] = strlist_to_enum('table', available_tables, None)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource

    dbname: str = Field(..., description='The database name.')
    user: str = Field(..., description='Your login username.')
    password: SecretStr = Field(None, description='Your login password')
    host: str = Field(None, description='IP address or hostname.')
    port: int = Field(..., description='port value of 5439 is specified by default')
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )
    cluster_identifier: str = Field(..., description='Name of the cluster')

    def get_connection_params(self):
        con_params = dict(
            dbname=self.dbname,
            user=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            connect_timeout=self.connect_timeout,
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _get_connection(self):
        """Establish a connection to an Amazon Redshift cluster."""
        connection = redshift_connector.connect(**self.get_connection_params())
        return connection

    def _get_cursor(self):
        connection = self._get_connection()
        cursor: redshift_connector.Cursor = connection.cursor()
        return cursor

    def _retrieve_data(self, query) -> pd.DataFrame:
        """Get data: tuple from table."""
        with redshift_connector.connect(**self.get_connection_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result: pd.DataFrame = cursor.fetch_dataframe()
        return result

    def get_status(self) -> ConnectorStatus:
        try:
            response = self._get_connection().describe_clusters(
                ClusterIdentifier=self.cluster_identifier
            )['Clusters']
            return ConnectorStatus(
                status=True, details=response[0]['ClusterStatus'] if response else None, error=None
            )
        except self._get_connection().exceptions.ClusterNotFoundException as error:
            return ConnectorStatus(status=False, details=None, error=error)
