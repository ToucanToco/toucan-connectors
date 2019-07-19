import pymssql

import pandas as pd
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector


class MSSQLDataSource(ToucanDataSource):
    # By default SQL Server selects the database which is set
    # as default for specific user
    database: str = None
    query: constr(min_length=1)


class MSSQLConnector(ToucanConnector):
    """
    Import data from Microsoft SQL Server.
    """
    data_source_model: MSSQLDataSource

    host: str
    user: str
    password: str = None
    port: int = None
    connect_timeout: int = None

    def get_connection_params(self, database):
        con_params = {
            'server': self.host,
            'user': self.user,
            'database': database,
            'password': self.password,
            'port': self.port,
            'login_timeout': self.connect_timeout,
            'as_dict': True
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, datasource):
        connection = pymssql.connect(**self.get_connection_params(datasource.database))
        df = pd.read_sql(datasource.query, con=connection)

        connection.close()
        return df
