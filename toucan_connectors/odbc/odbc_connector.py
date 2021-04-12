import pandas as pd
import pyodbc
from pydantic import Field, constr

from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class OdbcDataSource(ToucanDataSource):
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )


class OdbcConnector(ToucanConnector):
    """
    Import data through ODBC apis
    """

    data_source_model: OdbcDataSource

    connection_string: str = Field(..., description='The connection string')
    ansi: bool = False
    connect_timeout: int = None

    def get_connection_params(self):
        con_params = {
            'autocommit': False,
            'ansi': self.ansi,
            'timeout': self.connect_timeout,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, datasource: OdbcDataSource) -> pd.DataFrame:

        connection = pyodbc.connect(self.connection_string, **self.get_connection_params())
        df = pandas_read_sql(
            datasource.query, con=connection, params=datasource.parameters, convert_to_qmark=True
        )
        connection.close()
        return df
