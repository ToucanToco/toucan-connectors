import re

import pandas as pd
import pyodbc
from pydantic import Field, StringConstraints
from typing_extensions import Annotated

from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import PlainJsonSecretStr, ToucanConnector, ToucanDataSource

CLOUD_HOST = "database.windows.net"


class AzureMSSQLDataSource(ToucanDataSource):
    database: str = Field(..., description="The name of the database you want to query")
    query: Annotated[str, StringConstraints(min_length=1)] = Field(
        ..., description="You can write your SQL query here", widget="sql"
    )


class AzureMSSQLConnector(ToucanConnector, data_source_model=AzureMSSQLDataSource):
    """
    Import data from Microsoft Azure SQL Server.
    """

    host: str = Field(
        ...,
        description="The domain name (preferred option as more dynamic) or "
        "the hardcoded IP address of your database server",
    )

    user: str = Field(..., description="Your login username")
    password: PlainJsonSecretStr = Field("", description="Your login password")
    connect_timeout: int = Field(
        None,
        title="Connection timeout",
        description="You can set a connection timeout in seconds here, i.e. the maximum length of "
        "time you want to wait for the server to respond. None by default",
    )

    def get_connection_params(self, *, database=None):
        base_host = re.sub(f".{CLOUD_HOST}$", "", self.host)
        user = f"{self.user}@{base_host}" if "@" not in self.user else self.user

        if not self.password:
            self.password = PlainJsonSecretStr("")

        con_params = {
            "driver": "{ODBC Driver 17 for SQL Server}",
            "server": f"{base_host}.{CLOUD_HOST}",
            "database": database,
            "user": user,
            "password": self.password.get_secret_value(),
            "timeout": self.connect_timeout,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, datasource: AzureMSSQLDataSource) -> pd.DataFrame:
        connection = pyodbc.connect(**self.get_connection_params(database=datasource.database))

        query_params = datasource.parameters or {}
        df = pandas_read_sql(
            datasource.query,
            con=connection,
            params=query_params,
            convert_to_qmark=True,
            render_user=True,
        )

        connection.close()
        return df
