import re
from typing import Self

from pydantic import model_validator

from toucan_connectors.mssql.mssql_connector import CONNECTOR_OK as MSSQL_CONNECTOR_OK
from toucan_connectors.mssql.mssql_connector import MSSQLConnector, MSSQLDataSource

CONNECTOR_OK = MSSQL_CONNECTOR_OK

CLOUD_HOST = "database.windows.net"


class AzureMSSQLDataSource(MSSQLDataSource):
    """Same as MSSQLDataSource"""


class AzureMSSQLConnector(MSSQLConnector, data_source_model=AzureMSSQLDataSource):
    """
    Import data from Microsoft Azure SQL Server.
    """

    @model_validator(mode="after")
    def _sanitize_host_and_user(self) -> Self:
        base_host = re.sub(f".{CLOUD_HOST}$", "", self.host)
        host = f"{base_host}.{CLOUD_HOST}"
        user = f"{self.user}@{base_host}" if "@" not in self.user else self.user
        return self.model_copy(update={"host": host, "user": user})
