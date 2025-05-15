from logging import getLogger
from typing import TYPE_CHECKING, Annotated

from pydantic import Field, StringConstraints, create_model, model_validator

try:
    import pandas as pd
    from sqlalchemy.orm import Session

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from toucan_connectors.common import (
    convert_jinja_params_to_sqlalchemy_named,
    create_sqlalchemy_engine,
    pandas_read_sqlalchemy_query,
    pyformat_params_to_jinja,
    unnest_sql_jinja_parameters,
)
from toucan_connectors.toucan_connector import (
    PlainJsonSecretStr,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

if TYPE_CHECKING:
    import sqlalchemy as sa

_LOGGER = getLogger(__name__)


class MSSQLDataSource(ToucanDataSource):
    # By default SQL Server selects the database which is set
    # as default for specific user
    database: str | None = Field(
        None,
        description="The name of the database you want to query. "
        "By default SQL Server selects the user's default database",
    )
    table: Annotated[str, StringConstraints(min_length=1)] | None = Field(
        None,
        description='The name of the data table that you want to get (equivalent to "SELECT * FROM your_table")',
    )
    query: Annotated[str, StringConstraints(min_length=1)] | None = Field(
        None,
        description="You can write a custom query against your "
        "database here. It will take precedence over "
        'the "table" parameter above',
        json_schema_extra={"widget": "sql"},
    )

    @model_validator(mode="after")
    def table_or_query(self):
        if self.query is None and self.table is None:
            raise ValueError("'query' or 'table' must be set")
        elif self.query is None and self.table is not None:
            self.query = f"select * from {self.table};"
        return self

    @classmethod
    def get_form(cls, connector: "MSSQLConnector", current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `table` field
        """
        constraints = {}

        # We only wait for five seconds to make sure the AB don't think the connector form is broken.
        # Worst case scenario, there is no autocomplete and AB has to fill database by hand, but they're not stuck.
        sa_engine = connector._create_engine(database=current_config.get("database", "tempdb"), connect_timeout=5)

        # The user may not have rights to access `sys.databases`, in which case, we still want the form
        try:
            # Always add the suggestions for the available databases
            with Session(sa_engine) as session:
                with session.connection() as connection:
                    cursor = connection.connection.cursor()
                    cursor.execute("SELECT name FROM sys.databases")
                    res = cursor.fetchall()
                    available_dbs = [r[0] for r in res]

                    constraints["database"] = strlist_to_enum("database", available_dbs)

                    if "database" in current_config:
                        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;")
                        res = cursor.fetchall()
                        available_tables = [table_name for (table_name,) in res]
                        constraints["table"] = strlist_to_enum("table", available_tables, None)

                    cursor.close()
        except Exception as exc:
            _LOGGER.warning(f"unable to fetch database and/or table lists for {__name__}", exc_info=exc)

        return create_model("FormSchema", **constraints, __base__=cls).schema()  # type:ignore[call-overload]


class MSSQLConnector(ToucanConnector, data_source_model=MSSQLDataSource):
    """
    Import data from Microsoft SQL Server.
    """

    host: str = Field(
        ...,
        description="The domain name (preferred option as more dynamic) or "
        "the hardcoded IP address of your database server",
    )

    port: int | None = Field(None, description="The listening port of your database server")
    user: str = Field(..., description="Your login username")
    password: PlainJsonSecretStr | None = Field(None, description="Your login password")
    connect_timeout: int | None = Field(
        None,
        title="Connection timeout",
        description="You can set a connection timeout in seconds here, i.e. the maximum length "
        "of time you want to wait for the server to respond. None by default",
    )
    trust_server_certificate: bool = Field(
        False,
        title="Trust server certificate",
        description="This allows to disable server certificate validation, which can be "
        "required for custom and self-signed certificates. Connection is still encrypted.",
    )

    def _create_engine(self, database: str | None, connect_timeout: int | None = None) -> "sa.Engine":
        from sqlalchemy.engine import URL

        server = self.host
        if server == "localhost":
            server = "127.0.0.1"  # localhost is not understood by pyodbc
        if self.port is not None:
            server += f",{self.port}"

        query_params: dict[str, str] = {"driver": "ODBC Driver 18 for SQL Server"}

        if connect_timeout is None:
            connect_timeout = self.connect_timeout

        connect_args = {"timeout": connect_timeout} if connect_timeout else None

        if connect_timeout:
            query_params["timeout"] = str(connect_timeout)
        if self.trust_server_certificate:
            query_params["TrustServerCertificate"] = "yes"

        connection_url = URL.create(
            "mssql+pyodbc",
            username=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=server,
            database=database,
            query=query_params,
        )
        return create_sqlalchemy_engine(connection_url, connect_args=connect_args)

    def _retrieve_data(self, datasource: MSSQLDataSource) -> "pd.DataFrame":
        sa_engine = self._create_engine(database=datasource.database)

        # This should not happen as it is checked by the data sources' model validator
        if datasource.query is None:
            raise ValueError("'query' or 'table' must be set")

        # %()s -> {{}}
        jinja_query = pyformat_params_to_jinja(datasource.query)
        flattened_query, flattened_params = unnest_sql_jinja_parameters(jinja_query, datasource.parameters or {})
        final_query = convert_jinja_params_to_sqlalchemy_named(flattened_query)

        df = pandas_read_sqlalchemy_query(query=final_query, engine=sa_engine, params=flattened_params)

        return df
