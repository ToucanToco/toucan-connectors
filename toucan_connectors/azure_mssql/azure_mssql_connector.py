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

    @classmethod
    def get_form(cls, connector: "AzureMSSQLConnector", current_config: dict):
        """
        Method to retrieve the form with a current config
        Once the connector is set, we are often able to enforce some values depending
        on what the current `ToucanDataSource` config looks like

        By default, we simply return the model schema.
        """
        return cls.model_json_schema()
