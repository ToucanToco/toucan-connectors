from contextlib import suppress

import pandas as pd
import redshift_connector
from pydantic import create_model

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.redShift_common import RedshiftConnectorDbAuth, RsDataSource
from toucan_connectors.toucan_connector import (
    strlist_to_enum,
)
import boto3

client = boto3.client('redshift')


class RedshiftDataSource(RsDataSource):
    @classmethod
    def get_clusters(cls):
        response = client.describe_clusters()["Clusters"]
        clusters = [cluster["DBName"] for cluster in response]
        return clusters

    @classmethod
    def get_form(cls, connector: 'RedshiftConnector', current_config):
        constraints = {}

        with suppress(Exception):
            databases = cls.get_clusters()
            constraints['database'] = strlist_to_enum('database', databases)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class RedshiftConnector(RedshiftConnectorDbAuth):
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

    def _get_cursor(self, data_source: RedshiftDataSource):
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
