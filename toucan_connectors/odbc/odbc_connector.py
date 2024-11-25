from logging import getLogger
from typing import Annotated

from pydantic import Field, StringConstraints

try:
    import pandas as pd
    import pyodbc

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class OdbcDataSource(ToucanDataSource):
    query: Annotated[str, StringConstraints(min_length=1)] = Field(
        ..., description="You can write your SQL query here", widget="sql"
    )


class OdbcConnector(ToucanConnector, data_source_model=OdbcDataSource):
    """
    Import data through ODBC apis
    """

    connection_string: str = Field(..., description="The connection string")
    autocommit: bool = False
    ansi: bool = False
    connect_timeout: int = None

    def get_connection_params(self):
        con_params = {
            "autocommit": self.autocommit,
            "ansi": self.ansi,
            "timeout": self.connect_timeout,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, datasource: OdbcDataSource) -> "pd.DataFrame":
        connection = pyodbc.connect(self.connection_string, **self.get_connection_params())
        df = pandas_read_sql(datasource.query, con=connection, params=datasource.parameters, convert_to_qmark=True)
        connection.close()
        return df
