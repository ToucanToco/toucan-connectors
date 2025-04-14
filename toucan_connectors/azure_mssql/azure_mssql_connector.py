import re
from typing import Any

from pydantic import model_validator

from toucan_connectors.mssql.mssql_connector import CONNECTOR_OK as MSSQL_CONNECTOR_OK
from toucan_connectors.mssql.mssql_connector import MSSQLConnector, MSSQLDataSource

CONNECTOR_OK = MSSQL_CONNECTOR_OK

CLOUD_HOST = "database.windows.net"


class AzureMSSQLDataSource(MSSQLDataSource):
    """Same as MSSQLDataSource"""


class AzureMSSQLConnector(MSSQLConnector, data_source_model=AzureMSSQLDataSource):
    """Import data from Microsoft Azure SQL Server."""

    @model_validator(mode="before")
    @classmethod
    def _sanitize_host_and_user(cls, data: Any) -> dict:
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict, got {type(data)}: {data}")

        host = str(data.get("host", ""))
        user = str(data.get("user", ""))

        base_host = re.sub(f".{CLOUD_HOST}$", "", host)
        host = f"{base_host}.{CLOUD_HOST}"
        user = f"{user}@{base_host}" if "@" not in user else user
        return data | {"user": user, "host": host}
