import pandas as pd
import psycopg2 as pgsql
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class PostgresDataSource(ToucanDataSource):
    query: constr(min_length=1)
    parameters: dict = None


class PostgresConnector(ToucanConnector):
    """
    Import data from PostgreSQL.
    """
    type = 'Postgres'
    data_source_model: PostgresDataSource

    user: str
    host: str = None
    hostname: str = None
    charset: str = None
    db: str = None
    password: str = None
    port: int = None
    connect_timeout: int = None

    @property
    def connection_params(self):
        con_params = dict(
            user=self.user,
            host=self.host if self.host else self.hostname,
            client_encoding=self.charset,
            dbname=self.db,
            password=self.password,
            port=self.port,
            connect_timeout=self.connect_timeout
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def get_df(self, data_source):
        connection = pgsql.connect(**self.connection_params)

        query_params = data_source.parameters or {}
        df = pd.read_sql(data_source.query, con=connection, params=query_params)

        connection.close()

        return df
