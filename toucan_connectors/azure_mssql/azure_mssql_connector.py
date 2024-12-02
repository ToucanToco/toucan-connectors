import re
from logging import getLogger

try:
    import pandas as pd

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from typing import TYPE_CHECKING, Annotated

from pydantic import Field, SecretStr, StringConstraints

from toucan_connectors.common import (
    convert_jinja_params_to_sqlalchemy_named,
    create_sqlalchemy_engine,
    pandas_read_sqlalchemy_query,
)
from toucan_connectors.toucan_connector import PlainJsonSecretStr, ToucanConnector, ToucanDataSource

if TYPE_CHECKING:
    import sqlalchemy as sa

CLOUD_HOST = "database.windows.net"


class AzureMSSQLDataSource(ToucanDataSource):
    database: str = Field(..., description="The name of the database you want to query")
    query: Annotated[str, StringConstraints(min_length=1)] = Field(
        ..., description="You can write your SQL query here", json_schema_extra={"widget": "sql"}
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
    password: PlainJsonSecretStr = Field(SecretStr(""), description="Your login password")
    connect_timeout: int | None = Field(
        None,
        title="Connection timeout",
        description="You can set a connection timeout in seconds here, i.e. the maximum length of "
        "time you want to wait for the server to respond. None by default",
    )

    def _create_engine(self, database: str | None) -> "sa.Engine":
        from sqlalchemy.engine import URL

        base_host = re.sub(f".{CLOUD_HOST}$", "", self.host)
        host = f"{base_host}.{CLOUD_HOST}"
        user = f"{self.user}@{base_host}" if "@" not in self.user else self.user

        password = self.password.get_secret_value() if self.password else None

        query_params: dict[str, str] = {
            "driver": "ODBC Driver 17 for SQL Server",
        }
        if self.connect_timeout:
            query_params["timeout"] = str(self.connect_timeout)

        connection_url = URL.create(
            "mssql+pyodbc",
            username=user,
            password=password,
            host=host,
            database=database,
            query=query_params,
        )
        return create_sqlalchemy_engine(connection_url)

    def _retrieve_data(self, datasource: AzureMSSQLDataSource) -> "pd.DataFrame":
        sa_engine = self._create_engine(database=datasource.database)

        query_params = datasource.parameters or {}
        # {{param}} -> :param
        query = convert_jinja_params_to_sqlalchemy_named(datasource.query)

        df = pandas_read_sqlalchemy_query(query=query, engine=sa_engine, params=query_params)

        return df
