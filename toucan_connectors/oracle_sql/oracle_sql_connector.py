import cx_Oracle
import pandas as pd
from pydantic import Field, SecretStr, constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class OracleSQLDataSource(ToucanDataSource):
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )


class OracleSQLConnector(ToucanConnector):
    data_source_model: OracleSQLDataSource

    dsn: str = Field(
        ...,
        description='A path following the '
        '<a href="https://en.wikipedia.org/wiki/Data_source_name">DSN pattern</a>. '
        'The DSN host, port and service name are required.',
        examples=['localhost:80/service'],
    )
    user: str = Field(None, description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    encoding: str = Field(
        None, title='Charset', description='If you need to specify a specific character encoding.'
    )

    def get_connection_params(self):
        con_params = {
            'user': self.user,
            'password': self.password.get_secret_value() if self.password else None,
            'dsn': self.dsn,
            'encoding': self.encoding,
        }
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source: OracleSQLDataSource) -> pd.DataFrame:
        connection = cx_Oracle.connect(**self.get_connection_params())

        query = data_source.query[:-1] if data_source.query.endswith(';') else data_source.query
        df = pd.read_sql(query, con=connection)

        connection.close()

        return df
