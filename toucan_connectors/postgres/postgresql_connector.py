import pandas as pd
import psycopg2 as pgsql
from pydantic import Schema, SecretStr, constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class PostgresDataSource(ToucanDataSource):
    database: str = Schema(..., description='The name of the database you want to query')
    query: constr(min_length=1) = Schema(
        ..., description='You can write your SQL query here', widget='sql'
    )


class PostgresConnector(ToucanConnector):
    """
    Import data from PostgreSQL.
    """

    data_source_model: PostgresDataSource

    hostname: str = Schema(
        None,
        description='Use this parameter if you have a domain name (preferred option as more dynamic). '
        'If not, please use the "host" parameter',
    )
    host: str = Schema(
        None,
        description='Use this parameter if you have an IP address. '
        'If not, please use the "hostname" parameter (preferred option as more dynamic)',
    )
    port: int = Schema(None, description='The listening port of your database server')
    user: str = Schema(..., description='Your login username')
    password: SecretStr = Schema(None, description='Your login password')
    charset: str = Schema(None, description='If you need to specify a specific character encoding.')
    connect_timeout: int = Schema(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )

    def get_connection_params(self, *, database='postgres'):
        con_params = dict(
            user=self.user,
            host=self.host if self.host else self.hostname,
            client_encoding=self.charset,
            dbname=database,
            password=self.password.get_secret_value() if self.password else None,
            port=self.port,
            connect_timeout=self.connect_timeout,
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source):
        connection = pgsql.connect(**self.get_connection_params(database=data_source.database))

        query_params = data_source.parameters or {}
        df = pd.read_sql(data_source.query, con=connection, params=query_params)

        connection.close()

        return df
