from logging import getLogger
from typing import Optional

from pydantic import Field, StringConstraints, create_model
from typing_extensions import Annotated

from toucan_connectors.common import ConnectorStatus, pandas_read_sql
from toucan_connectors.postgres.utils import build_database_model_extraction_query, types
from toucan_connectors.toucan_connector import (
    DiscoverableConnector,
    PlainJsonSecretStr,
    TableInfo,
    ToucanConnector,
    ToucanDataSource,
    UnavailableVersion,
    VersionableEngineConnector,
    strlist_to_enum,
)

_LOGGER = getLogger(__name__)

try:
    import psycopg2 as pgsql

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    _LOGGER.warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False


DEFAULT_DATABASE = "postgres"


class PostgresDataSource(ToucanDataSource):
    database: str = Field(DEFAULT_DATABASE, description="The name of the database you want to query")
    query: Annotated[str | None, StringConstraints(min_length=1)] = Field(  # type:ignore[call-arg]
        None,
        description="You can write a custom query against your "
        "database here. It will take precedence over "
        'the "table" parameter',
        widget="sql",
    )
    query_object: dict | None = Field(  # type:ignore[pydantic-field]
        None,
        description="An object describing a simple select query" "This field is used internally",
        **{"ui.hidden": True},  # type:ignore[arg-type]
    )
    table: Annotated[str | None, StringConstraints(min_length=1)] = Field(
        None,
        description="The name of the data table that you want to " 'get (equivalent to "SELECT * FROM ' 'your_table")',
    )
    language: str = Field("sql", **{"ui.hidden": True})  # type:ignore[pydantic-field,arg-type]

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get("query")
        table = data.get("table")
        if query is None and table is None:
            raise ValueError("'query' or 'table' must be set")
        elif query is None and table is not None:
            self.query = f"select * from {table};"

    @classmethod
    def get_form(cls, connector: "PostgresConnector", current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `table` field
        """
        constraints = {}

        try:
            connection = pgsql.connect(
                **connector.get_connection_params(database=current_config.get("database", DEFAULT_DATABASE))
            )
            # # Always add the suggestions for the available databases
            with connection.cursor() as cursor:
                cursor.execute("""select datname from pg_database where datistemplate = false;""")
                res = cursor.fetchall()
                available_dbs = [db_name for (db_name,) in res]
                constraints["database"] = strlist_to_enum("database", available_dbs)

                if "database" in current_config:
                    cursor.execute(
                        """select table_schema, table_name from information_schema.tables
                    where table_schema NOT IN ('pg_catalog', 'information_schema');"""
                    )
                    res = cursor.fetchall()
                    available_tables = [table_name for (_, table_name) in res]
                    constraints["table"] = strlist_to_enum("table", available_tables, None)
        except Exception as exc:
            _LOGGER.warning(f"Exception occured when retrieving Postregs form schema: {exc}", exc_info=exc)

        return create_model("FormSchema", **constraints, __base__=cls).schema()  # type:ignore[call-overload]


class PostgresConnector(
    ToucanConnector,
    DiscoverableConnector,
    VersionableEngineConnector,
    data_source_model=PostgresDataSource,
):
    """
    Import data from PostgreSQL.
    """

    host: str | None = Field(None, description="The listening address of your database server (IP adress or hostname)")
    port: int | None = Field(None, description="The listening port of your database server")
    user: str = Field(..., description="Your login username")
    password: PlainJsonSecretStr | None = Field(None, description="Your login password")
    default_database: str = Field(DEFAULT_DATABASE, description="Your default database")

    charset: str | None = Field(None, description="If you need to specify a specific character encoding.")
    connect_timeout: int | None = Field(
        None,
        title="Connection timeout",
        description="You can set a connection timeout in seconds here, i.e. the maximal amount of "
        "time you want to wait for the server to respond. None by default",
    )

    include_materialized_views: bool = Field(
        False, description="Wether materialized views should be listed in the query builder or not."
    )

    def get_connection_params(self, *, database: str | None = None):
        con_params = {
            "user": self.user,
            "host": self.host,
            "client_encoding": self.charset,
            "dbname": database if database is not None else self.default_database,
            "password": self.password.get_secret_value() if self.password else None,
            "port": self.port,
            "connect_timeout": self.connect_timeout,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source):
        connection = pgsql.connect(**self.get_connection_params(database=data_source.database))

        query_params = data_source.parameters or {}
        df = pandas_read_sql(data_source.query, con=connection, params=query_params, adapt_params=True)

        connection.close()

        return df

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = [
            "Host resolved",
            "Port opened",
            "Connected to PostgreSQL",
            "Authenticated",
            "Default Database connection",
        ]
        ok_checks = [(c, True) for i, c in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(c, None) for i, c in enumerate(checks) if i > index]
        return ok_checks + [new_check] + not_validated_checks

    def get_status(self) -> ConnectorStatus:
        # Check hostname
        try:
            self.check_hostname(self.host)
        except (Exception, pgsql.Error) as e:
            return ConnectorStatus(status=False, details=self._get_details(0, False), error=str(e))

        # Check port
        try:
            self.check_port(self.host, self.port)
        except (Exception, pgsql.Error) as e:
            return ConnectorStatus(status=False, details=self._get_details(1, False), error=str(e))

        # Check basic access
        try:
            pgsql.connect(**self.get_connection_params())
        except (Exception, pgsql.Error) as e:
            error_code = e.args[0]

            # Can't connect to full URI
            if "Connection refused" in error_code:
                return ConnectorStatus(status=False, details=self._get_details(2, False), error=e.args[0])

            # Wrong user/password
            else:
                return ConnectorStatus(status=False, details=self._get_details(3, False), error=e.args[0])

        # Basic db query
        try:
            connection = pgsql.connect(**self.get_connection_params(database=self.default_database))
            with connection.cursor() as cursor:
                cursor.execute("""select 1;""")
        except (Exception, pgsql.Error) as e:
            return ConnectorStatus(status=False, details=self._get_details(4, False), error=e.args[0])

        return ConnectorStatus(status=True, details=self._get_details(4, True), error=None)

    def describe(self, data_source: PostgresDataSource):
        connection = pgsql.connect(**self.get_connection_params(database=data_source.database))
        with connection.cursor() as cursor:
            assert data_source.query is not None, "no query provided"
            cursor.execute(f"""SELECT * FROM ({data_source.query.replace(';','')}) AS q LIMIT 0;""")
            res = cursor.description
        return {r.name: types.get(r.type_code) for r in res}

    def get_model(
        self,
        db_name: str | None = None,
        schema_name: str | None = None,
        table_name: str | None = None,
        exclude_columns: bool = False,
    ) -> list[TableInfo]:
        """Retrieves the database tree structure using current connection"""
        available_dbs = self._list_db_names() if db_name is None else [db_name]
        databases_tree = []
        for db in available_dbs:
            try:
                databases_tree += self._list_tables_info(
                    database_name=db, schema_name=schema_name, table_name=table_name, exclude_columns=exclude_columns
                )
            except pgsql.OperationalError as exc:
                _LOGGER.warning(f"An error occured when retrieving table info for database {db}: {exc}", exc_info=exc)
        return DiscoverableConnector.format_db_model(databases_tree)

    def get_model_with_info(
        self,
        db_name: str | None = None,
        schema_name: str | None = None,
        table_name: str | None = None,
        exclude_columns: bool = False,
    ) -> tuple[list[TableInfo], dict]:
        """Retrieves the database tree structure using current connection"""
        available_dbs = self._list_db_names() if db_name is None else [db_name]
        databases_tree = []
        failed_databases = []
        for db in available_dbs:
            try:
                databases_tree += self._list_tables_info(
                    database_name=db, schema_name=schema_name, table_name=table_name, exclude_columns=exclude_columns
                )
            except pgsql.OperationalError:
                failed_databases.append(db)

        tables_info = DiscoverableConnector.format_db_model(databases_tree)
        metadata = {}
        if failed_databases:
            metadata["info"] = {"Could not reach databases": failed_databases}
        return (tables_info, metadata)

    def _list_db_names(self) -> list[str]:
        connection = pgsql.connect(**self.get_connection_params(database=self.default_database))
        with connection.cursor() as cursor:
            cursor.execute("""select datname from pg_database where datistemplate = false;""")
            return [db_name for (db_name,) in cursor.fetchall()]

    def _list_tables_info(
        self, *, database_name: str | None, schema_name: str | None, table_name: str | None, exclude_columns: bool
    ) -> list[tuple]:
        connection = pgsql.connect(
            **self.get_connection_params(database=self.default_database if not database_name else database_name)
        )
        with connection.cursor() as cursor:
            cursor.execute(
                build_database_model_extraction_query(
                    db_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    include_materialized_views=self.include_materialized_views,
                    exclude_columns=exclude_columns,
                )
            )
            return cursor.fetchall()

    def get_engine_version(self) -> tuple:
        """
        We try to get the PostgreSQL version by running a query with our connection
        """
        connection = pgsql.connect(**self.get_connection_params(database=self.default_database))

        with connection.cursor() as cursor:
            cursor.execute("SELECT CURRENT_SETTING('server_version');")
            version = cursor.fetchone()
            try:
                return super()._format_version(str(version[0]))
            except (TypeError, IndexError) as exc:
                raise UnavailableVersion from exc
