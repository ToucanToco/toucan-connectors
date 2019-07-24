import pandas as pd
import psycopg2 as pgsql
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class PostgresDataSource(ToucanDataSource):
    database: str
    query: constr(min_length=1)


class PostgresConnector(ToucanConnector):
    """
    Import data from PostgreSQL.
    """
    data_source_model: PostgresDataSource

    user: str
    host: str = None
    hostname: str = None
    charset: str = None
    password: str = None
    port: int = None
    connect_timeout: int = None

    def get_connection_params(self, *, database='postgres'):
        con_params = dict(
            user=self.user,
            host=self.host if self.host else self.hostname,
            client_encoding=self.charset,
            dbname=database,
            password=self.password,
            port=self.port,
            connect_timeout=self.connect_timeout
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source):
        connection = pgsql.connect(
            **self.get_connection_params(database=data_source.database)
        )

        query_params = data_source.parameters or {}
        df = pd.read_sql(data_source.query, con=connection, params=query_params)

        connection.close()

        return df
