from typing import Any, Optional

import numpy as np
import pandas as pd
import pymysql
from cached_property import cached_property_with_ttl
from pydantic import Field, SecretStr, constr, create_model
from pymysql.constants import CR, ER

from toucan_connectors.common import ConnectorStatus, pandas_read_sql
from toucan_connectors.toucan_connector import (
    DiscoverableConnector,
    TableInfo,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)


def handle_date_0(df: pd.DataFrame) -> pd.DataFrame:
    # Mysql driver doesnt translate date '0000-00-00 00:00:00'
    # to a datetime, so the Series has a 'object' dtype instead of 'datetime'.
    # This util fixes this behaviour, by replacing it with NaT.
    return df.replace({'0000-00-00 00:00:00': pd.NaT}).infer_objects()


class MySQLDataSource(ToucanDataSource):
    """
    Either `query` or `table` are required, both at the same time are not supported.
    """

    database: str = Field(..., description='The name of the database you want to query')
    table: str = Field(
        None, **{'ui.hidden': True}
    )  # To avoid previous config migrations, won't be used
    query: constr(min_length=1) = Field(
        None,
        description='You can write a custom query against your '
        'database here. It will take precedence over '
        'the "table" parameter',
        widget='sql',
    )
    query_object: dict = Field(
        None,
        description='An object describing a simple select query' 'This field is used internally',
        **{'ui.hidden': True},
    )
    language: str = Field('sql', **{'ui.hidden': True})

    @classmethod
    def get_form(cls, connector: 'MySQLConnector', current_config: dict[str, Any]) -> dict:
        return create_model(
            'FormSchema',
            database=strlist_to_enum('database', connector.available_dbs),
            __base__=cls,
        ).schema()


class MySQLConnector(ToucanConnector, DiscoverableConnector):
    """
    Import data from MySQL database.
    """

    data_source_model: MySQLDataSource

    host: str = Field(
        ...,
        description='The domain name (preferred option as more dynamic) or '
        'the hardcoded IP address of your database server',
    )
    port: int = Field(None, description='The listening port of your database server')
    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    charset: str = Field(
        'utf8mb4',
        title='Charset',
        description='Character encoding. You should generally let the default "utf8mb4" here.',
    )
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, '
        'i.e. the maximum length of time you want to wait '
        'for the server to respond. None by default',
    )

    class Config:
        underscore_attrs_are_private = True
        keep_untouched = (cached_property_with_ttl,)

    def _list_db_names(self) -> list[str]:
        connection = pymysql.connect(
            **self.get_connection_params(cursorclass=None, database='mysql')
        )
        # Always add the suggestions for the available databases
        with connection.cursor() as cursor:
            cursor.execute('SHOW DATABASES;')
            res = cursor.fetchall()
            return [
                db_name
                for (db_name,) in res
                if db_name not in ('information_schema', 'mysql', 'performance_schema')
            ]

    def _get_project_structure(self) -> list[TableInfo]:
        connection = pymysql.connect(
            **self.get_connection_params(cursorclass=None, database='mysql')
        )
        # Always add the suggestions for the available databases
        with connection.cursor() as cursor:
            cursor.execute(build_database_model_extraction_query())
            return cursor.fetchall()

    @cached_property_with_ttl(ttl=10)
    def available_dbs(self) -> list[str]:
        return self._list_db_names()

    @cached_property_with_ttl(ttl=60)
    def project_tree(self) -> list[TableInfo]:
        return self._get_project_structure()

    def get_connection_params(self, *, database=None, cursorclass=pymysql.cursors.DictCursor):
        conv = pymysql.converters.conversions.copy()
        conv[246] = float
        con_params = {
            'host': self.host,
            'user': self.user,
            'password': self.password.get_secret_value() if self.password else None,
            'port': self.port,
            'database': database,
            'charset': self.charset,
            'connect_timeout': self.connect_timeout,
            'conv': conv,
            'cursorclass': cursorclass,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = ['Hostname resolved', 'Port opened', 'Host connection', 'Authenticated']
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
            pymysql.connect(**self.get_connection_params())
        except pymysql.err.OperationalError as e:
            error_code = e.args[0]

            # Can't connect to full URI
            if error_code == CR.CR_CONN_HOST_ERROR:
                return ConnectorStatus(
                    status=False, details=self._get_details(2, False), error=e.args[1]
                )

            # Wrong user/password
            if error_code == ER.ACCESS_DENIED_ERROR:
                return ConnectorStatus(
                    status=False, details=self._get_details(3, False), error=e.args[1]
                )

        return ConnectorStatus(status=True, details=self._get_details(3, True), error=None)

    def get_model(self) -> list[Any]:
        """Retrieves the database tree structure using current connection"""
        return DiscoverableConnector.format_db_model(self.project_tree)

    @staticmethod
    def decode_df(df):
        """
        Used to change bytes columns to string columns
        (can be moved to be applied for all connectors if needed)
        It retrieves all the string columns and converts them all together.
        The string columns become nan columns so we remove them from the result,
        we keep the rest and insert it back to the dataframe
        """
        str_df = df.select_dtypes([np.object])
        if str_df.empty:
            return df

        str_df = str_df.stack().str.decode('utf8').unstack().dropna(axis=1, how='all')
        for col in str_df.columns:
            df[col] = str_df[col]
        return df

    def _retrieve_data(self, datasource):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].
        """

        connection = pymysql.connect(**self.get_connection_params(database=datasource.database))

        # ----- Prepare -----
        # As long as frontend is build queries with '"' we need to replace them
        query = datasource.query.replace('"', '`')
        MySQLConnector.logger.debug(f'Executing query : {datasource.query}')
        query_params = datasource.parameters or {}

        df = pandas_read_sql(query, con=connection, params=query_params)
        df = self.decode_df(df)
        df = handle_date_0(df)
        connection.close()
        return df


class InvalidQuery(Exception):
    """raised when a query is invalid"""


def build_database_model_extraction_query() -> str:
    """
    table_schema is selected 2x because the frontend components need it but
    mysql provide it only for SQL-92 standard's compliance see:
    https://dba.stackexchange.com/questions/3774/what-is-the-point-of-the-table-catalog-column-in-information-schema-tables
    """
    return """select t.table_schema AS "database", t.table_schema as "schema",
    CASE WHEN t.table_type = 'BASE TABLE' THEN 'table' ELSE lower(t.table_type) end as "type",
    t.table_name as "name", json_arrayagg(json_object('name', c.column_name, 'type', c.data_type))
    as columns from information_schema.tables t
    inner join information_schema.columns c on t.table_name = c.table_name
    where t.table_type in ('BASE TABLE', 'VIEW') and
    t.table_schema not in ('mysql', 'information_schema', 'performance_schema', 'sys')
    group by t.table_schema, t.table_catalog, t.table_name, t.table_type;"""
