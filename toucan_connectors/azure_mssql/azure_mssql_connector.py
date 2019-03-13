import pyodbc
import re

import pandas as pd
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector

CLOUD_HOST = 'database.windows.net'


class AzureMSSQLDataSource(ToucanDataSource):
    query: constr(min_length=1)


class AzureMSSQLConnector(ToucanConnector):
    """
    Import data from Microsoft Azure SQL Server.
    """
    type = 'AzureMSSQL'
    data_source_model: AzureMSSQLDataSource

    host: str
    user: str
    password: str
    db: str
    connect_timeout: int = None

    @property
    def connection_params(self):
        base_host = re.sub(f'.{CLOUD_HOST}$', '', self.host)
        user = f'{self.user}@{base_host}' if '@' not in self.user else self.user

        con_params = {
            'driver': '{ODBC Driver 17 for SQL Server}',
            'server': f'{base_host}.{CLOUD_HOST}',
            'user': user,
            'database': self.db,
            'password': self.password,
            'timeout': self.connect_timeout
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def get_df(self, datasource: AzureMSSQLDataSource) -> pd.DataFrame:
        connection = pyodbc.connect(**self.connection_params)

        df = pd.read_sql(datasource.query, con=connection)

        connection.close()
        return df
