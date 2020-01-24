import pandas as pd
import psycopg2 as pgsql
from pydantic import Field, SecretStr, constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class PostgresDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )


class PostgresConnector(ToucanConnector):
    """
    Import data from PostgreSQL.
    """

    data_source_model: PostgresDataSource

    hostname: str = Field(
        None,
        description='Use this parameter if you have a domain name (preferred option as more dynamic). '
        'If not, please use the "host" parameter',
    )
    host: str = Field(
        None,
        description='Use this parameter if you have an IP address. '
        'If not, please use the "hostname" parameter (preferred option as more dynamic)',
    )
    port: int = Field(None, description='The listening port of your database server')
    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    charset: str = Field(None, description='If you need to specify a specific character encoding.')
    connect_timeout: int = Field(
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
