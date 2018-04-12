import pandas as pd
import psycopg2 as pgsql
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class PostgresDataSource(ToucanDataSource):
    query: constr(min_length=1)


class PostgresConnector(ToucanConnector):
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

    def get_df(self, data_source):
        connection = pgsql.connect(
            user=self.user,
            host=self.host if self.host else self.hostname,
            client_encoding=self.charset,
            dbname=self.db,
            password=self.password,
            port=self.port,
            connect_timeout=self.connect_timeout
        )
        cursor = connection.cursor()
        query = data_source.query

        df = pd.read_sql(query, con=connection)

        connection.close()

        return df
