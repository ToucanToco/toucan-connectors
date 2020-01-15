import pandas as pd
import pymysql
from pydantic import Field, SecretStr, constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class GoogleCloudMySQLDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )


class GoogleCloudMySQLConnector(ToucanConnector):
    """
    Import data from Google Cloud MySQL database.
    """

    data_source_model: GoogleCloudMySQLDataSource

    host: str = Field(
        ...,
        description='The domain name (preferred option as more dynamic) or '
        'the hardcoded IP address of your database server',
    )

    port: int = Field(None, description='The listening port of your database server')
    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(..., description='Your login password')
    charset: str = Field(
        'utf8mb4',
        title='Charset',
        description='Character encoding. You should generally let the default "utf8mb4" here.',
    )
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length '
        'of time you want to wait for the server to respond. None by default',
    )

    def get_connection_params(self, *, database=None):
        conv = pymysql.converters.conversions.copy()
        conv[246] = float
        con_params = {
            'host': self.host,
            'user': self.user,
            'password': self.password.get_secret_value() if self.password else None,
            'port': self.port,
            'database': database,
            'charset': self.charset,
            'connect_timeout': self.connect_timeout,
            'conv': conv,
            'cursorclass': pymysql.cursors.DictCursor,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source: GoogleCloudMySQLDataSource) -> pd.DataFrame:
        connection = pymysql.connect(**self.get_connection_params(database=data_source.database))

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
