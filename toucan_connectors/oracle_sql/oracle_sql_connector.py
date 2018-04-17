import cx_Oracle
import pandas as pd
from pydantic import DSN

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class OracleSQLDataSource(ToucanDataSource):
    query: str


class OracleSQLConnector(ToucanConnector):
    type = 'oracle_sql'
    data_source_model: OracleSQLDataSource

    dsn: DSN
    user: str = None
    password: str = None
    encoding: str = None

    @property
    def connection_params(self):
        con_params = {
            'user': self.user,
            'password': self.password,
            'dsn': self.dsn,
            'encoding': self.encoding
        }
        return {k: v for k, v in con_params.items() if v is not None}

    def get_df(self, data_source: OracleSQLDataSource) -> pd.DataFrame:
        connection = cx_Oracle.connect(**self.connection_params)

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
