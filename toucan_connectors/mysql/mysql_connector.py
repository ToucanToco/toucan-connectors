import logging
import os
from collections.abc import Generator
from enum import Enum
from itertools import groupby as groupby
from tempfile import NamedTemporaryFile
from typing import Annotated, Any

from cached_property import cached_property_with_ttl
from pydantic import ConfigDict, Field, StringConstraints, create_model, model_validator

from toucan_connectors.common import (
    ConnectorStatus,
    convert_to_printf_templating_style,
    pandas_read_sql,
    pyformat_params_to_jinja,
    unnest_sql_jinja_parameters,
)
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
from toucan_connectors.utils.pem import sanitize_spaces_pem

_LOGGER = logging.getLogger(__name__)

try:
    import pandas as pd
    import pymysql
    from pymysql.constants import CR, ER

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    _LOGGER.warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

try:
    _DEFAULT_CURSOR_CLASS = None
    if pd.__version__.startswith("2"):
        _DEFAULT_CURSOR_CLASS = pymysql.cursors.Cursor
    else:
        _DEFAULT_CURSOR_CLASS = pymysql.cursors.DictCursor
except Exception as exc:
    _LOGGER.warning(f"Could not figure out pandas version, using DictCursor: {exc}", exc_info=exc)


def handle_date_0(df: "pd.DataFrame") -> "pd.DataFrame":
    # Mysql driver doesnt translate date '0000-00-00 00:00:00'
    # to a datetime, so the Series has a 'object' dtype instead of 'datetime'.
    # This util fixes this behaviour, by replacing it with NaT.
    return df.replace({"0000-00-00 00:00:00": pd.NaT}).infer_objects()


class NoQuerySpecified(Exception):
    def __init__(self) -> None:
        super().__init__("no query was specified")


class MySQLDataSource(ToucanDataSource):
    """
    Either `query` or `table` are required, both at the same time are not supported.
    """

    database: str = Field(..., description="The name of the database you want to query")
    follow_relations: bool | None = Field(  # type: ignore[call-overload]
        None,
        **{"ui.hidden": True},
        description="Deprecated, kept for compatibility purpose with old data sources configs",
    )
    table: str | None = Field(None, **{"ui.hidden": True})  # type: ignore[call-overload]
    query: Annotated[str, StringConstraints(min_length=1)] | None = Field(  # type: ignore[call-overload]
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
    language: str = Field("sql", **{"ui.hidden": True})  # type: ignore[call-overload]

    @classmethod
    def get_form(cls, connector: "MySQLConnector", current_config: dict[str, Any]) -> dict:
        return create_model(
            "FormSchema",
            database=strlist_to_enum("database", connector.available_dbs),
            __base__=cls,
        ).schema()


_DATABASE_MODEL_EXTRACTION_QUERY = (
    # table_schema is selected twice because the frontend components needs it but
    # mysql provides it only for compliance with the SQL-92 standard.
    # https://dba.stackexchange.com/questions/3774/what-is-the-point-of-the-table-catalog-column-in-information-schema-tables
    "SELECT t.table_schema AS 'database', t.table_schema AS 'schema', "
    # Table type and name
    "CASE WHEN t.table_type = 'BASE TABLE' THEN 'table' ELSE LOWER(t.table_type) END AS table_type, t.table_name, "
    # Columns from the columns table
    "c.column_name, c.data_type FROM information_schema.tables t INNER JOIN information_schema.columns c "
    # Inner join on table name
    "ON t.table_name = c.table_name AND t.table_schema = c.table_schema "
    # Filtering on concrete tables/views
    "WHERE t.table_type in ('BASE TABLE', 'VIEW') AND t.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') "  # noqa: E501
)


class SSLMode(str, Enum):
    VERIFY_IDENTITY = "VERIFY_IDENTITY"
    VERIFY_CA = "VERIFY_CA"
    REQUIRED = "REQUIRED"


def prepare_query_and_params_for_pymysql(query: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Prepares the query and params to a format that is well supported by pymysql.

    Since version 1.1.1, pymysql does not support dicts in parameters anymore. They cause an
    exception, even if the parameter is not used. This function sanitizes the query and
    parameters through the following steps:

    1. Convert params in pyformat style to jinja style, i.e. %(param)s to {{ param }}. This is done
       to support both queries interpolated with jinja (the modern way to do it), and queries with
       pyformat params (we might have some leftovers of that). See the PEP for details on supported
       param styles: https://peps.python.org/pep-0249/#paramstyle
    2. Find all jinja variables
    3. Rename the variables in the query string and build a parameter dict mapping the renamed
       variables to what they evaluate to (we will evaluate the expressions with jinja)
    4. convert the query back to pyformat param style
    """
    # %()[sdf] -> {{}}
    jinja_query = pyformat_params_to_jinja(query)

    substituted_query, substituted_params = unnest_sql_jinja_parameters(jinja_query, params)
    # {{}} -> %()s
    final_query = convert_to_printf_templating_style(substituted_query)
    return final_query, substituted_params


class MySQLConnector(
    ToucanConnector,
    DiscoverableConnector,
    VersionableEngineConnector,
    data_source_model=MySQLDataSource,
):
    """
    Import data from MySQL database.
    """

    host: str = Field(
        ...,
        description="The domain name (preferred option as more dynamic) or "
        "the hardcoded IP address of your database server",
    )
    port: int | None = Field(None, description="The listening port of your database server")
    user: str = Field(..., description="Your login username")
    password: PlainJsonSecretStr | None = Field(None, description="Your login password")
    charset: str = Field(
        "utf8mb4",
        title="Charset",
        description='Character encoding. You should generally let the default "utf8mb4" here.',
    )
    charset_collation: str | None = Field(
        None,
        title="Charset collation",
        description="The charset's collation for the connections to the server."
        "Only set it here if your tables do not use your server's default value.",
    )
    connect_timeout: int | None = Field(
        None,
        title="Connection timeout",
        description="You can set a connection timeout in seconds here, "
        "i.e. the maximum length of time you want to wait "
        "for the server to respond. None by default",
    )
    # SSL options
    ssl_ca: PlainJsonSecretStr | None = Field(
        None,
        description="The CA certificate content in PEM format to use to connect to the MySQL "
        "server. Equivalent of the --ssl-ca option of the MySQL client",
    )
    ssl_cert: PlainJsonSecretStr | None = Field(
        None,
        description="The X509 certificate content in PEM format to use to connect to the MySQL "
        "server. Equivalent of the --ssl-cert option of the MySQL client",
    )
    ssl_key: PlainJsonSecretStr | None = Field(
        None,
        description="The X509 certificate key content in PEM format to use to connect to the MySQL "
        "server. Equivalent of the --ssl-key option of the MySQL client",
    )
    ssl_mode: SSLMode | None = Field(
        None,
        description="SSL Mode to use to connect to the MySQL server. "
        "Equivalent of the --ssl-mode option of the MySQL client. Must be set in order to use SSL",
    )
    model_config = ConfigDict(ignored_types=(cached_property_with_ttl,))

    @model_validator(mode="after")
    def ssl_key_validator(self) -> "MySQLConnector":
        # if one is present, the other one should be specified
        if self.ssl_cert is not None and self.ssl_key is None:
            raise ValueError('SSL option "ssl_key" should be specified if "ssl_cert" is provided !')
        elif self.ssl_key is not None and self.ssl_cert is None:
            raise ValueError('SSL option "ssl_cert" should be specified if "ssl_key" is provided !')

        return self

    def _sanitize_ssl_params(self) -> dict[str, Any]:
        params = {}
        if self.ssl_mode in (SSLMode.VERIFY_CA, SSLMode.VERIFY_IDENTITY):
            for ssl_opt in ("ssl_ca", "ssl_key", "ssl_cert"):
                opt = getattr(self, ssl_opt)
                if opt is None:
                    continue
                secret = opt.get_secret_value()
                if secret.strip() != "":
                    params[ssl_opt] = sanitize_spaces_pem(secret)
        return params

    def _list_db_names(self) -> list[str]:
        connection = self._connect(cursorclass=None, database=None)
        # Always add the suggestions for the available databases
        with connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES;")
            res = cursor.fetchall()
            return [
                db_name for (db_name,) in res if db_name not in ("information_schema", "mysql", "performance_schema")
            ]

    def _get_project_structure(self, db_name: str | None = None) -> Generator[TableInfo, None, None]:
        connection = self._connect(cursorclass=None, database=db_name)

        extraction_query = _DATABASE_MODEL_EXTRACTION_QUERY
        if db_name:
            # If the db name is specified, filter on it
            extraction_query += f"AND t.table_schema = '{db_name}'"
        extraction_query += ";"

        with connection.cursor() as cursor:
            cursor.execute(extraction_query)
            results = cursor.fetchall()

        column_names = ("database", "schema", "table_type", "table_name", "columns")
        # Grouping by DB name, schema name, Table type, Table name
        for group, grouper in groupby(sorted(results), key=lambda x: x[:4]):
            col_info = [{"name": x[4], "type": x[5]} for x in grouper]
            yield dict(zip(column_names, group + (col_info,), strict=False))

    @cached_property_with_ttl(ttl=10)
    def available_dbs(self) -> list[str]:
        return self._list_db_names()

    def project_tree(self, db_name: str | None = None) -> list[TableInfo]:
        return list(self._get_project_structure(db_name=db_name))

    def get_connection_params(self, *, database: str | None = None, cursorclass=_DEFAULT_CURSOR_CLASS):
        conv = pymysql.converters.conversions.copy()
        conv[246] = float
        con_params = {
            "host": self.host,
            "user": self.user,
            "password": self.password.get_secret_value() if self.password else None,
            "port": self.port,
            "database": database,
            "charset": self.charset,
            "connect_timeout": self.connect_timeout,
            "conv": conv,
            "cursorclass": cursorclass,
            "collation": self.charset_collation,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _connect(self, *, database: str | None = None, cursorclass=None) -> "pymysql.Connection":
        connection_params = self.get_connection_params(database=database, cursorclass=cursorclass)
        if self.ssl_mode is not None:
            connection_params |= {
                "ssl_disabled": False,
                # Verify the server's certificate. This one is actually required by pymysql, as no
                # SSL context will be created otherwise:
                # https://github.com/PyMySQL/PyMySQL/blob/main/pymysql/connections.py#L266
                "ssl_verify_cert": True,
            }

        if self.ssl_mode in (SSLMode.VERIFY_CA, SSLMode.VERIFY_IDENTITY):
            ssl_params = self._sanitize_ssl_params()
            ssl_files = []
            for ssl_opt in ("ssl_ca", "ssl_key", "ssl_cert"):
                if ssl_opt in ssl_params:
                    ssl_opt_file = NamedTemporaryFile(prefix=ssl_opt, delete=False)
                    ssl_opt_file.write(ssl_params[ssl_opt].encode())
                    ssl_opt_file.seek(0)

                    connection_params[ssl_opt] = ssl_opt_file.name
                    ssl_files.append(ssl_opt_file)

            connection_params["ssl_verify_identity"] = self.ssl_mode == SSLMode.VERIFY_IDENTITY

            try:
                connection = pymysql.connect(**connection_params)
            finally:
                for ssl_file in ssl_files:
                    ssl_file.close()
                    os.unlink(ssl_file.name)  # needed otherwise file is not closed.
            return connection
        return pymysql.connect(**connection_params)

    @staticmethod
    def _get_details(index: int, status: bool | None):
        checks = ["Hostname resolved", "Port opened", "Host connection", "Authenticated"]
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
            self._connect()
        except pymysql.err.OperationalError as e:
            error_code = e.args[0]

            # Wrong user/password
            if error_code == ER.ACCESS_DENIED_ERROR:
                return ConnectorStatus(status=False, details=self._get_details(3, False), error=e.args[1])

            # Can't connect to full URI
            if error_code == CR.CR_CONN_HOST_ERROR:
                return ConnectorStatus(status=False, details=self._get_details(2, False), error=e.args[1])

            # There can be other errors, in which case we consider that there is something wrong
            # with the host connection
            _LOGGER.warning(f"Unexpected MySQL error code {error_code}: {e}", exc_info=e)
            return ConnectorStatus(status=False, details=self._get_details(2, False), error=e.args[1])

        return ConnectorStatus(status=True, details=self._get_details(3, True), error=None)

    def get_model(
        self,
        db_name: str | None = None,
        schema_name: str | None = None,
        table_name: str | None = None,
        exclude_columns: bool = False,
    ) -> list[TableInfo]:
        """Retrieves the database tree structure using current connection"""
        return DiscoverableConnector.format_db_model(self.project_tree(db_name=db_name))

    @staticmethod
    def decode_df(df):
        """
        Used to change bytes columns to string columns
        (can be moved to be applied for all connectors if needed)
        It retrieves all the string columns and converts them all together.
        The string columns become nan columns so we remove them from the result,
        we keep the rest and insert it back to the dataframe
        """
        str_df = df.select_dtypes([object])
        if str_df.empty:
            return df

        str_df = str_df.stack().str.decode("utf8").unstack().dropna(axis=1, how="all")
        for col in str_df.columns:
            df[col] = str_df[col]
        return df

    def _retrieve_data(self, datasource):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].
        """

        if not datasource.query or not datasource.query.strip():
            raise NoQuerySpecified

        connection = self._connect(database=datasource.database)

        query_params = datasource.parameters or {}
        query = datasource.query

        # ----- Prepare -----
        prepared_query, prepared_params = prepare_query_and_params_for_pymysql(query, query_params)

        # As long as frontend builds queries with '"' we need to replace them
        backticked_query = prepared_query.replace('"', "`")
        MySQLConnector.logger.debug(
            f"Executing query : {query} with params {query_params}. "
            f"Prepared query: {prepared_query}. Prepared params: {prepared_params}"
        )

        df = pandas_read_sql(backticked_query, con=connection, params=prepared_params)
        df = self.decode_df(df)
        df = handle_date_0(df)
        connection.close()
        return df

    def get_engine_version(self) -> tuple:
        """
        We try to get the MySQL version by running a query with our connection
        """
        connection = pymysql.connect(**self.get_connection_params())

        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            try:
                return super()._format_version(version["VERSION()"])
            except (TypeError, KeyError) as exc:
                raise UnavailableVersion from exc


class InvalidQuery(Exception):
    """raised when a query is invalid"""
