from contextlib import suppress
from typing import Any

import clickhouse_driver
from pydantic import Field, StringConstraints, create_model
from pydantic.json_schema import DEFAULT_REF_TEMPLATE, GenerateJsonSchema, JsonSchemaMode
from typing_extensions import Annotated

from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import (
    PlainJsonSecretStr,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)


class ClickhouseDataSource(ToucanDataSource):
    database: str = Field(None, description="The name of the database you want to query")
    query: Annotated[str, StringConstraints(min_length=1)] = Field(
        None,
        description="You can write a custom query against your "
        "database here. It will take precedence over "
        'the "table" parameter above',
        widget="sql",
    )
    table: Annotated[str, StringConstraints(min_length=1)] = Field(
        None,
        description="The name of the data table that you want to " 'get (equivalent to "SELECT * FROM ' 'your_table")',
    )

    @classmethod
    def model_json_schema(
        cls,
        by_alias: bool = True,
        ref_template: str = DEFAULT_REF_TEMPLATE,
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation",
    ) -> dict[str, Any]:
        schema = super().model_json_schema(
            by_alias=by_alias,
            ref_template=ref_template,
            schema_generator=schema_generator,
            mode=mode,
        )
        keys = schema["properties"].keys()
        prio_keys = [
            "database",
            "table",
            "query",
            "parameters",
        ]
        new_keys = prio_keys + [k for k in keys if k not in prio_keys]
        schema["properties"] = {k: schema["properties"][k] for k in new_keys}
        return schema

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get("query")
        table = data.get("table")
        if query is None and table is None:
            raise ValueError("'query' or 'table' must be set")
        elif query is None and table is not None:
            self.query = f"select * from {table};"

    @classmethod
    def get_form(cls, connector: "ClickhouseConnector", current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `table` field
        """
        constraints = {}

        with suppress(Exception):
            connection = clickhouse_driver.connect(connector.get_connection_url())
            # Always add the suggestions for the available databases

            with connection.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                res = cursor.fetchall()
                available_dbs = [db_name for (db_name,) in res if db_name != "system"]
                constraints["database"] = strlist_to_enum("database", available_dbs)

                if "database" in current_config:
                    cursor.execute(
                        f"""SELECT name FROM system.tables WHERE database = '{current_config["database"]}'"""
                    )
                    res = cursor.fetchall()
                    available_tables = [table[0] for table in res]
                    constraints["table"] = strlist_to_enum("table", available_tables, None)

        return create_model("FormSchema", **constraints, __base__=cls).schema()


class ClickhouseConnector(ToucanConnector, data_source_model=ClickhouseDataSource):
    """
    Import data from Clickhouse.
    """

    host: str = Field(
        None,
        description="Use this parameter if you have an IP address. "
        'If not, please use the "hostname" parameter (preferred option as more dynamic)',
    )
    port: int = Field(None, description="The listening port of your database server")
    user: str = Field(..., description="Your login username")
    password: PlainJsonSecretStr = Field("", description="Your login password")
    ssl_connection: bool = Field(False, description="Create a SSL wrapped TCP connection")

    def get_connection_url(self, *, database="default"):
        proto = "clickhouses" if self.ssl_connection else "clickhouse"
        return f'{proto}://{self.user}:{self.password.get_secret_value() if self.password else ""}@{self.host}:{self.port}/{database}'  # noqa: E501

    def _retrieve_data(self, data_source):
        connection = clickhouse_driver.connect(self.get_connection_url(database=data_source.database))
        query_params = data_source.parameters or {}
        query = data_source.query if data_source.query else f"select * from {data_source.table} limit 50;"
        df = pandas_read_sql(query, con=connection, params=query_params, adapt_params=True)

        connection.close()

        return df
