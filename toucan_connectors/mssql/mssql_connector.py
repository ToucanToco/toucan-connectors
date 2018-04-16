import pymssql

import pandas as pd
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector


class MSSQLDataSource(ToucanDataSource):
    query: constr(min_length=1)


class MSSQLConnector(ToucanConnector):
    """
    Import data from Microsoft SQL Server.
    """
    type = 'MSSQL'
    data_source_model: MSSQLDataSource

    host: str
    user: str
    db: str = None
    password: str = None
    port: int = None
    connect_timeout: int = None

    @property
    def connection_params(self):
        con_params = {
            'server': self.host,
            'user': self.user,
            'database': self.db,
            'password': self.password,
            'port': self.port,
            'login_timeout': self.connect_timeout,
            'as_dict': True
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def get_df(self, datasource):
        connection = pymssql.connect(**self.connection_params)

        df = pd.read_sql(datasource.query, con=connection)

        connection.close()
        return df
