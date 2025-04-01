import re

from toucan_connectors.mssql.mssql_connector import MSSQLConnector, MSSQLDataSource

CLOUD_HOST = "database.windows.net"


class AzureMSSQLDataSource(MSSQLDataSource):
    """Same as MSSQLDataSource"""


class AzureMSSQLConnector(MSSQLConnector, data_source_model=AzureMSSQLDataSource):
    """
    Import data from Microsoft Azure SQL Server.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        base_host = re.sub(f".{CLOUD_HOST}$", "", self.host)
        self.host = f"{base_host}.{CLOUD_HOST}"
        self.user = f"{self.user}@{base_host}" if "@" not in self.user else self.user
