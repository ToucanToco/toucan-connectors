import pandas as pd
import pymysql
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector


class GoogleCloudMySQLDataSource(ToucanDataSource):
    database: str
    query: constr(min_length=1)


class GoogleCloudMySQLConnector(ToucanConnector):
    """
    Import data from Google Cloud MySQL database.
    """
    data_source_model: GoogleCloudMySQLDataSource

    host: str
    user: str
    password: str
    port: int = None
    charset: str = 'utf8mb4'
    connect_timeout: int = None

    def get_connection_params(self, *, database=None):
        conv = pymysql.converters.conversions.copy()
        conv[246] = float
        con_params = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'port': self.port,
            'database': database,
            'charset': self.charset,
            'connect_timeout': self.connect_timeout,
            'conv': conv,
            'cursorclass': pymysql.cursors.DictCursor
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source: GoogleCloudMySQLDataSource) -> pd.DataFrame:
        connection = pymysql.connect(
            **self.get_connection_params(database=data_source.database)
        )

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
