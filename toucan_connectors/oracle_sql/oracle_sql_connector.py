import cx_Oracle
import pandas as pd
from pydantic import DSN

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class OracleDataSource(ToucanDataSource):
    query: str


class OracleConnector(ToucanConnector):
    type = 'oracle'
    data_source_model: OracleDataSource

    dsn: DSN = None
    user: str = None
    password: str = None
    host: str
    port: str
    db: str
    encoding: str = None

    @property
    def connection_params(self):
        con_params = {
            'user': self.user,
            'password': self.password,
            'dsn': f'{self.host}:{self.port}/{self.db}',
            'encoding': self.encoding
        }
        return {k: v for k, v in con_params.items() if v is not None}

    def get_df(self, data_source: OracleDataSource) -> pd.DataFrame:
        connection = cx_Oracle.connect(**self.connection_params)

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
