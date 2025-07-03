from logging import getLogger
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import Field, StringConstraints, create_model

from toucan_connectors.common import (
    ConnectorStatus,
    convert_jinja_params_to_sqlalchemy_named,
    create_sqlalchemy_engine,
    pandas_read_sqlalchemy_query,
    pyformat_params_to_jinja,
    unnest_sql_jinja_parameters,
)
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
    from sqlalchemy import text as sa_text
    from sqlalchemy.engine import URL
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import Session

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    _LOGGER.warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

if TYPE_CHECKING:
    import sqlalchemy as sa

DEFAULT_DATABASE = "postgres"


class PostgresDataSource(ToucanDataSource):
    database: str = Field(DEFAULT_DATABASE, description="The name of the database you want to query")
    query: Annotated[str | None, StringConstraints(min_length=1)] = Field(  # type: ignore[call-overload]
        None,
        description="You can write a custom query against your "
        "database here. It will take precedence over "
        'the "table" parameter',
        widget="sql",
    )
    query_object: dict | None = Field(  # type: ignore[call-overload]
        None,
        description="An object describing a simple select query. This field is used internally",
        **{"ui.hidden": True},
    )
    table: Annotated[str | None, StringConstraints(min_length=1)] = Field(
        None,
        description='The name of the data table that you want to get (equivalent to "SELECT * FROM your_table")',
    )
    language: str = Field("sql", **{"ui.hidden": True})  # type: ignore[call-overload]

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
            sa_engine = connector.create_engine(database=current_config.get("database", DEFAULT_DATABASE))
            with Session(sa_engine) as session:
                # # Always add the suggestions for the available databases
                result = session.execute(
                    sa_text("""SELECT datname FROM pg_database WHERE datistemplate = false;""")
                ).fetchall()
                available_dbs = [db_name for (db_name,) in result]
                constraints["database"] = strlist_to_enum("database", available_dbs)
                if "database" in current_config:
                    result = session.execute(
                        sa_text(
                            """SELECT table_schema, table_name FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema');"""
                        )
                    ).fetchall()
                    available_tables = [table_name for (_, table_name) in result]
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

    def create_engine(self, database: str | None, connect_timeout: int | None = None) -> "sa.Engine":
        server = self.host
        if connect_timeout is None:
            connect_timeout = self.connect_timeout
        connect_args = {"connect_timeout": connect_timeout} if connect_timeout else None

        query_params: dict[str, str] = {}
        if self.charset:
            query_params["client_encoding"] = self.charset

        connection_url = URL.create(
            "postgresql+psycopg",
            username=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=server,
            port=self.port,
            database=database or self.default_database,
            query=query_params,
        )
        return create_sqlalchemy_engine(connection_url, connect_args)

    def _retrieve_data(self, data_source):
        sa_engine = self.create_engine(database=data_source.database)
        jinja_query = pyformat_params_to_jinja(data_source.query)
        flattened_query, flattened_params = unnest_sql_jinja_parameters(jinja_query, data_source.parameters or {})
        final_query = convert_jinja_params_to_sqlalchemy_named(flattened_query)
        return pandas_read_sqlalchemy_query(query=final_query, engine=sa_engine, params=flattened_params)

    @staticmethod
    def _get_details(index: int, status: bool | None):
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
        except Exception as e:
            return ConnectorStatus(status=False, details=self._get_details(0, False), error=str(e))

        # Check port
        try:
            self.check_port(self.host, self.port)
        except Exception as e:
            return ConnectorStatus(status=False, details=self._get_details(1, False), error=str(e))

        # Check basic access
        try:
            sa_engine = self.create_engine(database=self.default_database, connect_timeout=1)
            conn = sa_engine.connect()
            conn.close()
        except Exception as e:
            error_code = e.args[0]
            # Can't connect to full URI
            if "Connection refused" in error_code:
                return ConnectorStatus(status=False, details=self._get_details(2, False), error=e.args[0])
            # Wrong user/password
            else:
                return ConnectorStatus(status=False, details=self._get_details(3, False), error=e.args[0])
        # Basic db query
        try:
            with Session(sa_engine) as session:
                session.execute(sa_text("""select 1;"""))

        except Exception as e:
            return ConnectorStatus(status=False, details=self._get_details(4, False), error=e.args[0])

        return ConnectorStatus(status=True, details=self._get_details(4, True), error=None)

    def describe(self, data_source: PostgresDataSource):
        """Describes fields of the requested table"""
        assert data_source.query is not None, "no query provided"
        sa_engine = self.create_engine(database=data_source.database)
        with Session(sa_engine) as session:
            # We have to use DBAPI Cursor to get the description
            cursor = session.connection().connection.cursor()
            res = cursor.execute(f"""SELECT * FROM ({data_source.query.replace(";", "")}) AS q LIMIT 0;""").description
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
        databases_tree: list[TableInfo] = []
        for db in available_dbs:
            try:
                databases_tree += self._list_tables_info(
                    database_name=db, schema_name=schema_name, table_name=table_name, exclude_columns=exclude_columns
                )
            except OperationalError as exc:
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
        databases_tree: list[tuple] = []
        failed_databases = []
        for db in available_dbs:
            try:
                databases_tree += self._list_tables_info(
                    database_name=db, schema_name=schema_name, table_name=table_name, exclude_columns=exclude_columns
                )
            except OperationalError:
                failed_databases.append(db)

        tables_info = DiscoverableConnector.format_db_model(databases_tree)
        metadata = {}
        if failed_databases:
            metadata["info"] = {"Could not reach databases": failed_databases}
        return (tables_info, metadata)

    def _list_db_names(self) -> list[str]:
        sa_engine = self.create_engine(database=self.default_database)
        with Session(sa_engine) as session:
            results = session.execute(
                sa_text("""SELECT datname FROM pg_database WHERE datistemplate = false;""")
            ).fetchall()
            return [db_name for (db_name,) in results]

    def _list_tables_info(
        self, *, database_name: str | None, schema_name: str | None, table_name: str | None, exclude_columns: bool
    ) -> list[Any]:
        sa_engine = self.create_engine(database=database_name)
        with Session(sa_engine) as session:
            res = session.execute(
                sa_text(
                    build_database_model_extraction_query(
                        db_name=database_name,
                        schema_name=schema_name,
                        table_name=table_name,
                        include_materialized_views=self.include_materialized_views,
                        exclude_columns=exclude_columns,
                    )
                )
            ).fetchall()
            return [r.tuple() for r in res]

    def get_engine_version(self) -> tuple:
        """
        We try to get the PostgreSQL version by running a query with our connection
        """
        sa_engine = self.create_engine(database=self.default_database, connect_timeout=1)
        with Session(sa_engine) as session:
            version = session.execute(sa_text("SELECT CURRENT_SETTING('server_version');")).fetchone()
            try:
                if version is None:
                    raise UnavailableVersion
                return super()._format_version(str(version.tuple()[0]))
            except (TypeError, IndexError) as exc:
                raise UnavailableVersion from exc
