from contextlib import suppress
from logging import getLogger
from typing import Annotated

from pydantic import Field, StringConstraints, create_model

try:
    import cx_Oracle
    import pandas as pd

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False


from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import (
    PlainJsonSecretStr,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)


class OracleSQLDataSource(ToucanDataSource):
    query: Annotated[str, StringConstraints(min_length=1)] = Field(
        None, description="You can write your SQL query here", widget="sql"
    )
    table: Annotated[str, StringConstraints(min_length=1)] = Field(
        None,
        description='The name of the data table that you want to get (equivalent to "SELECT * FROM "your_table")',
    )

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get("query")
        table = data.get("table")
        if query is None and table is None:
            raise ValueError("'query' or 'table' must be set")
        elif query is None and table is not None:
            self.query = f"SELECT * FROM {table}"

    @classmethod
    def get_form(cls, connector: "OracleSQLConnector", current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `table` field
        """
        constraints = {}

        with suppress(Exception):
            connection = cx_Oracle.connect(**connector.get_connection_params())
            with connection.cursor() as cursor:
                cursor.execute("""select table_name from ALL_TABLES""")
                res = cursor.fetchall()
                # Filter tables starting with an '_' because strlist_to_enum cannot
                # set attributes starting with '_'
                available_tables = [table_name for (table_name,) in res if table_name[0] != "_"]
                constraints["table"] = strlist_to_enum("table", available_tables, None)

        return create_model("FormSchema", **constraints, __base__=cls).schema()


class OracleSQLConnector(ToucanConnector, data_source_model=OracleSQLDataSource):
    dsn: str = Field(
        ...,
        description="A path following the "
        '<a href="https://en.wikipedia.org/wiki/Data_source_name" target="_blank">DSN pattern</a>. '
        "The DSN host, port and service name are required.",
        examples=["localhost:80/service"],
    )
    user: str = Field(None, description="Your login username")
    password: PlainJsonSecretStr = Field(None, description="Your login password")
    encoding: str = Field(None, title="Charset", description="If you need to specify a specific character encoding.")

    def get_connection_params(self):
        con_params = {
            "user": self.user,
            "password": self.password.get_secret_value() if self.password else None,
            "dsn": self.dsn,
            "encoding": self.encoding,
        }
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source: OracleSQLDataSource) -> "pd.DataFrame":
        connection = cx_Oracle.connect(**self.get_connection_params())

        query = data_source.query[:-1] if data_source.query.endswith(";") else data_source.query
        query_params = data_source.parameters or {}
        df = pandas_read_sql(
            query,
            con=connection,
            params=query_params,
            convert_to_numeric=True,
            convert_to_printf=True,
        )

        connection.close()

        return df
