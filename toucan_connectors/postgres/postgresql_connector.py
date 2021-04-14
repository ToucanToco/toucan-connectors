from contextlib import suppress

import psycopg2 as pgsql
from pydantic import Field, SecretStr, constr, create_model

from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum


class PostgresDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    query: constr(min_length=1) = Field(
        None,
        description='You can write a custom query against your '
        'database here. It will take precedence over '
        'the "table" parameter above',
        widget='sql',
    )
    table: constr(min_length=1) = Field(
        None,
        description='The name of the data table that you want to '
        'get (equivalent to "SELECT * FROM '
        'your_table")',
    )

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get('query')
        table = data.get('table')
        if query is None and table is None:
            raise ValueError("'query' or 'table' must be set")
        elif query is None and table is not None:
            self.query = f'select * from {table};'

    @classmethod
    def get_form(cls, connector: 'PostgresConnector', current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `table` field
        """
        constraints = {}

        with suppress(Exception):
            connection = pgsql.connect(
                **connector.get_connection_params(
                    database=current_config.get('database', 'postgres')
                )
            )
            # # Always add the suggestions for the available databases
            with connection.cursor() as cursor:
                cursor.execute("""select datname from pg_database where datistemplate = false;""")
                res = cursor.fetchall()
                available_dbs = [db_name for (db_name,) in res]
                constraints['database'] = strlist_to_enum('database', available_dbs)

                if 'database' in current_config:
                    cursor.execute(
                        """select table_schema, table_name from information_schema.tables
                    where table_schema NOT IN ('pg_catalog', 'information_schema');"""
                    )
                    res = cursor.fetchall()
                    available_tables = [table_name for (_, table_name) in res]
                    constraints['table'] = strlist_to_enum('table', available_tables)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class PostgresConnector(ToucanConnector):
    """
    Import data from PostgreSQL.
    """

    data_source_model: PostgresDataSource

    hostname: str = Field(
        None,
        description='Use this parameter if you have a domain name (preferred option as more dynamic). '
        'If not, please use the "host" parameter',
    )
    host: str = Field(
        None,
        description='Use this parameter if you have an IP address. '
        'If not, please use the "hostname" parameter (preferred option as more dynamic)',
    )
    port: int = Field(None, description='The listening port of your database server')
    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    charset: str = Field(None, description='If you need to specify a specific character encoding.')
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )

    def get_connection_params(self, *, database='postgres'):
        con_params = dict(
            user=self.user,
            host=self.host if self.host else self.hostname,
            client_encoding=self.charset,
            dbname=database,
            password=self.password.get_secret_value() if self.password else None,
            port=self.port,
            connect_timeout=self.connect_timeout,
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, data_source):
        connection = pgsql.connect(**self.get_connection_params(database=data_source.database))

        query_params = data_source.parameters or {}
        df = pandas_read_sql(
            data_source.query, con=connection, params=query_params, adapt_params=True
        )

        connection.close()

        return df
