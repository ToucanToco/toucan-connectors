import pandas as pd
import pyodbc
from pydantic import Field, SecretStr, constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class MSSQLDataSource(ToucanDataSource):
    # By default SQL Server selects the database which is set
    # as default for specific user
    database: str = Field(
        None,
        description='The name of the database you want to query. '
        "By default SQL Server selects the user's default database",
    )
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )


class MSSQLConnector(ToucanConnector):
    """
    Import data from Microsoft SQL Server.
    """

    data_source_model: MSSQLDataSource

    host: str = Field(
        ...,
        description='The domain name (preferred option as more dynamic) or '
        'the hardcoded IP address of your database server',
    )

    port: int = Field(None, description='The listening port of your database server')
    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length '
        'of time you want to wait for the server to respond. None by default',
    )

    def get_connection_params(self, database):
        server = self.host
        if server == 'localhost':
            server = '127.0.0.1'  # localhost is not understood by pyodbc
        if self.port is not None:
            server += f',{self.port}'
        con_params = {
            'driver': '{ODBC Driver 17 for SQL Server}',
            'server': server,
            'database': database,
            'user': self.user,
            'password': self.password.get_secret_value() if self.password else None,
            'timeout': self.connect_timeout,
            'as_dict': True,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, datasource):
        connection = pyodbc.connect(**self.get_connection_params(datasource.database))
        df = pd.read_sql(datasource.query, con=connection)

        connection.close()
        return df
