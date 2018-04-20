import pandas as pd
import pymysql
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector


class GoogleCloudMySQLDataSource(ToucanDataSource):
    query: constr(min_length=1)


class GoogleCloudMySQLConnector(ToucanConnector):
    """
    Import data from Google Cloud MySQL database.
    """
    type = 'GoogleCloudMySQL'
    data_source_model: GoogleCloudMySQLDataSource

    host: str
    user: str
    db: str
    password: str
    port: int = None
    charset: str = 'utf8mb4'
    connect_timeout: int = None

    @property
    def connection_params(self):
        conv = pymysql.converters.conversions.copy()
        conv[246] = float
        con_params = {
            'host': self.host,
            'user': self.user,
            'database': self.db,
            'password': self.password,
            'port': self.port,
            'charset': self.charset,
            'connect_timeout': self.connect_timeout,
            'conv': conv,
            'cursorclass': pymysql.cursors.DictCursor
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def get_df(self, data_source: GoogleCloudMySQLDataSource) -> pd.DataFrame:
        connection = pymysql.connect(**self.connection_params)

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
