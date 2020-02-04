import pyodbc

import pandas as pd
from pydantic import constr

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector


class ODBCDataSource(ToucanDataSource):
    query: constr(min_length=1)


class ODBCConnector(ToucanConnector):
    """
    Import data from Microsoft Azure SQL Server.
    """
    data_source_model: ODBCDataSource

    connection_string: str
    autocommit: bool = False
    ansi: bool = False
    connect_timeout: int = None

    def get_connection_params(self):
        con_params = {
            'autocommit': self.autocommit,
            'ansi': self.ansi,
            'timeout': self.connect_timeout
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, datasource: ODBCDataSource) -> pd.DataFrame:
        connection = pyodbc.connect(self.connection_string, **self.get_connection_params())

        query_params = datasource.parameters or {}

        # <FIXME: UNSAFE>
        query_params = {k: f'"{v}"' for k, v in query_params.items()}  # add enclosing quotes
        q = nosql_apply_parameters_to_query(datasource.query, query_params)
        df = pd.read_sql(q, con=connection)
        # </UNSAFE>

        # # Apparently ODBC only supports qmark paramstyle
        # # (https://www.python.org/dev/peps/pep-0249/#paramstyle)
        # # so it expects a list of values, not a dict:
        # query_params = list(query_params.values())
        # df = pd.read_sql(datasource.query, con=connection, params=query_params)

        connection.close()
        return df
