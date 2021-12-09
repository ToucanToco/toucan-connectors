from typing import Dict

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr, create_model
from redshift_connector.error import InterfaceError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum

DATABASE_QUERY = """select oid as database_id, datname as database_name, datallowconn as allow_connect from pg_database order by oid;"""


class RedshiftDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')

    query: Dict = Field(None, description='An object describing a simple select query')

    @classmethod
    def get_form(cls, connector: 'RedshiftConnector', current_config):
        constraints = {}
        try:
            connection = redshift_connector.connect(**connector.get_connection_params())
            with connection.cursor() as cursor:
                cursor.execute(DATABASE_QUERY)
                res = cursor.fetchall()
                available_dbs = [db for element in res for db in element if isinstance(db, str)]
                constraints['databases'] = strlist_to_enum('databases', available_dbs)
            return create_model('FormSchema', **constraints, __base__=cls).schema()
        except Exception as err:
            model = create_model('FormSchema', __base__=None).schema()
            model['error'] = err
            return model


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource

    user: str = Field(..., description='Your login username.')
    password: SecretStr = Field(None, description='Your login password')
    host: str = Field(None, description='IP address or hostname.')
    port: int = Field(..., description='The listening port of your Redshift Database')
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )
    cluster_identifier: str = Field(..., description='Name of the cluster')

    def get_connection_params(self):
        con_params = dict(
            database=self.data_source_model.database,
            user=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            connect_timeout=self.connect_timeout,
            cluster_identifier=self.cluster_identifier,
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

    def _retrieve_data(self) -> pd.DataFrame:
        """Get data: tuple from table."""
        with redshift_connector.connect(**self.get_connection_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.data_source_model.query)
                result: pd.DataFrame = cursor.fetch_dataframe()
        return result

    def get_status(self) -> ConnectorStatus:
        try:
            with redshift_connector.connect(**self.get_connection_params()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(DATABASE_QUERY)
                    result: pd.DataFrame = cursor.fetch_dataframe()
                    print(f'{result=}')
                    return ConnectorStatus(status=True, details=None, error=None)
        except InterfaceError as err:
            return ConnectorStatus(status=False, error=err)
