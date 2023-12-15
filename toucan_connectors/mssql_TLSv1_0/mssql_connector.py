import logging
from contextlib import suppress

import pyodbc
from pydantic import Field, StringConstraints, create_model
from typing_extensions import Annotated

from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import (
    PlainJsonSecretStr,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)


class MSSQLDataSource(ToucanDataSource):
    # By default SQL Server selects the database which is set
    # as default for specific user
    database: str = Field(
        None,
        description="The name of the database you want to query. "
        "By default SQL Server selects the user's default database",
    )
    table: Annotated[str, StringConstraints(min_length=1)] = Field(
        None,
        description="The name of the data table that you want to " 'get (equivalent to "SELECT * FROM ' 'your_table")',
    )
    query: Annotated[str, StringConstraints(min_length=1)] = Field(
        None,
        description="You can write a custom query against your "
        "database here. It will take precedence over "
        'the "table" parameter above',
        widget="sql",
    )

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get("query")
        table = data.get("table")
        if query is None and table is None:
            raise ValueError("'query' or 'table' must be set")
        elif query is None and table is not None:
            self.query = f"select * from {table};"

    @classmethod
    def get_form(cls, connector: "MSSQLConnector", current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `table` field
        """
        constraints = {}

        with suppress(Exception):
            connection = pyodbc.connect(
                **connector.get_connection_params(database=current_config.get("database", "tempdb"))
            )
            # # Always add the suggestions for the available databases
            with connection.cursor() as cursor:
                cursor.execute("SELECT name FROM sys.databases;")
                res = cursor.fetchall()
                available_dbs = [r[0] for r in res]
                constraints["database"] = strlist_to_enum("database", available_dbs)

                if "database" in current_config:
                    cursor.execute("SELECT TABLE_NAME FROM  INFORMATION_SCHEMA.TABLES;")
                    res = cursor.fetchall()
                    available_tables = [table_name for (table_name,) in res]
                    constraints["table"] = strlist_to_enum("table", available_tables, None)

        return create_model("FormSchema", **constraints, __base__=cls).schema()


class MSSQLConnector(ToucanConnector, data_source_model=MSSQLDataSource):
    """
    Import data from Microsoft SQL Server.
    """

    host: str = Field(
        ...,
        description="The domain name (preferred option as more dynamic) or "
        "the hardcoded IP address of your database server",
    )

    port: int = Field(None, description="The listening port of your database server")
    user: str = Field(..., description="Your login username")
    password: PlainJsonSecretStr = Field(None, description="Your login password")
    connect_timeout: int = Field(
        None,
        title="Connection timeout",
        description="You can set a connection timeout in seconds here, i.e. the maximum length "
        "of time you want to wait for the server to respond. None by default",
    )

    def get_connection_params(self, database):
        server = self.host
        if server == "localhost":
            server = "127.0.0.1"  # localhost is not understood by pyodbc
        if self.port is not None:
            server += f",{self.port}"
        con_params = {
            "driver": "{ODBC Driver 17 for SQL Server}",
            "server": server,
            "database": database,
            "user": self.user,
            "password": self.password.get_secret_value() if self.password else None,
            "timeout": self.connect_timeout,
            "as_dict": True,
        }
        # remove None values
        ret = {k: v for k, v in con_params.items() if v is not None}
        ret_print = {k: v for k, v in con_params.items() if v is not None and k != "password"}
        logging.getLogger(__name__).info(f"Retrieve parameters for connection {ret_print}")
        return ret

    def _retrieve_data(self, datasource):
        connection = pyodbc.connect(**self.get_connection_params(datasource.database))

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
