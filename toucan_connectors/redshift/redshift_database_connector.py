import logging
import re
from contextlib import suppress
from enum import Enum
from functools import cached_property
from typing import Any

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr, create_model, root_validator, validator
from pydantic.types import constr

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.pagination import build_pagination_info
from toucan_connectors.redshift.utils import build_database_model_extraction_query, types_map
from toucan_connectors.sql_query_helper import SqlQueryHelper
from toucan_connectors.toucan_connector import (
    DataSlice,
    DiscoverableConnector,
    TableInfo,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

TABLE_QUERY = """SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public';"""

DESCRIBE_QUERY = """SELECT * FROM ({column}) AS q LIMIT 0;"""

DEFAULT_DATABASE = 'dev'

ORDERED_KEYS = [
    'type',
    'name',
    'host',
    'port',
    'default_database',
    'authentication_method',
    'user',
    'password',
    'cluster_identifier',
    'db_user',
    'connect_timeout',
    'access_key_id',
    'secret_access_key',
    'session_token',
    'profile',
    'region',
    'enable_tcp_keepalive',
]

logger = logging.getLogger(__name__)


class AuthenticationMethod(str, Enum):
    DB_CREDENTIALS: str = 'db_credentials'
    AWS_CREDENTIALS: str = 'aws_credentials'
    AWS_PROFILE: str = 'aws_profile'


class AuthenticationMethodError(str, Enum):
    DB_CREDENTIALS: str = f'User & Password are required for {AuthenticationMethod.DB_CREDENTIALS}'
    AWS_CREDENTIALS: str = f'AccessKeyId, SecretAccessKey & db_user are required for {AuthenticationMethod.AWS_CREDENTIALS}'
    AWS_PROFILE: str = f'Profile & db_user are required for {AuthenticationMethod.AWS_PROFILE}'
    UNKNOWN: str = 'Unknown AuthenticationMethod'


class RedshiftDataSource(ToucanDataSource):
    database: str = Field(
        DEFAULT_DATABASE, description='The name of the database you want to query'
    )
    query: constr(min_length=1) = Field(
        None,
        description='You can write a custom query against your '
        'database here. It will take precedence over '
        'the table parameter',
        widget='sql',
    )
    query_object: dict[str, Any] = Field(
        None,
        description='An object describing a simple select query, this field is used internally',
        **{'ui.hidden': True},
    )
    language: str = Field('sql', **{'ui.hidden': True})

    @classmethod
    def get_form(cls, connector: 'RedshiftConnector', current_config: dict[str, Any]):
        """
        Method to retrieve the form with a current config
        Once the connector is set, we are able to give suggestions for the `database` field
        """
        default_db = current_config.get('database', DEFAULT_DATABASE)
        return create_model(
            'FormSchema',
            database=strlist_to_enum('database', connector.available_dbs, default_db),
            __base__=cls,
        ).schema()


class RedshiftConnector(ToucanConnector, DiscoverableConnector):
    data_source_model: RedshiftDataSource
    authentication_method: AuthenticationMethod = Field(
        None,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your redshift data source',
        **{'ui': {'checkbox': False}},
    )
    host: str = Field(..., description='The hostname of the Amazon Redshift cluster')
    port: int = Field(5439, description='The listening port of your Redshift Database')
    default_database: str = Field(
        DEFAULT_DATABASE, description='The name of the database instance to connect to'
    )
    user: str | None = Field(
        None, description='The username to use for authentication with the Amazon Redshift cluster'
    )
    password: SecretStr | None = Field(
        None, description='The password to use for authentication with the Amazon Redshift cluster'
    )

    db_user: str | None = Field(None, description='The user ID to use with Amazon Redshift')
    cluster_identifier: str | None = Field(
        None, description='The cluster identifier of the Amazon Redshift cluster'
    )

    connect_timeout: int | None = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )
    # True by default to match redshift_connector kwargs syntax
    enable_tcp_keepalive: bool = Field(
        True,
        title='Enable TCP keep-alive',
        description='You may disable TCP keep-alive by unticking this option. Disabling might be '
        'required for long-running queries or if you are behind a firewall',
    )

    access_key_id: str | None = Field(None, description='The access key id of your aws account.')
    secret_access_key: SecretStr | None = Field(
        None, description='The secret access key of your aws account.'
    )
    session_token: str | None = Field(None, description='Your session token')
    profile: str | None = Field(None, description='AWS profile')
    region: str | None = Field(None, description='The region in which there is your aws account.')

    class Config:
        underscore_attrs_are_private = True
        keep_untouched = (cached_property,)

        @staticmethod
        def schema_extra(schema: dict[str, Any]) -> None:
            schema['properties'] = {k: schema['properties'][k] for k in ORDERED_KEYS}

    @cached_property
    def available_dbs(self) -> list[str]:
        return self._list_db_names()

    @validator('host')
    def host_validator(cls, v):
        return re.sub(r'^https?://', '', v)

    @root_validator
    def check_requirements(cls, values):
        mode = values.get('authentication_method')
        if mode == AuthenticationMethod.DB_CREDENTIALS.value:
            # TODO: Partial check due to missing context in some operations (Missing: password)
            user = values.get('user')
            if user is None:
                raise ValueError(AuthenticationMethodError.DB_CREDENTIALS.value)
        elif mode == AuthenticationMethod.AWS_CREDENTIALS.value:
            # TODO: Partial check due to missing context in some operations (Missing: secret_access_key)
            access_key_id, db_user = (
                values.get('access_key_id'),
                values.get('db_user'),
            )
            if access_key_id is None or db_user is None:
                raise ValueError(AuthenticationMethodError.AWS_CREDENTIALS.value)
        elif mode == AuthenticationMethod.AWS_PROFILE.value:
            profile, db_user = (values.get('profile'), values.get('db_user'))
            if profile is None or db_user is None:
                raise ValueError(AuthenticationMethodError.AWS_PROFILE.value)
        else:
            raise ValueError(AuthenticationMethodError.UNKNOWN.value)
        return values

    def _get_connection_params(self, database) -> dict[str, Any]:
        con_params = dict(
            database=database,
            host=self.host,
            port=self.port,
            timeout=self.connect_timeout,
            cluster_identifier=self.cluster_identifier,
            tcp_keepalive=self.enable_tcp_keepalive,
        )
        if self.authentication_method == AuthenticationMethod.DB_CREDENTIALS.value:
            con_params['user'] = self.user
            con_params['password'] = self.password.get_secret_value() if self.password else None
        elif self.authentication_method == AuthenticationMethod.AWS_CREDENTIALS.value:
            con_params['iam'] = True
            con_params['db_user'] = self.db_user
            con_params['access_key_id'] = self.access_key_id
            con_params['secret_access_key'] = (
                self.secret_access_key.get_secret_value() if self.secret_access_key else None
            )
            con_params['session_token'] = self.session_token
            con_params['region'] = self.region
        elif self.authentication_method == AuthenticationMethod.AWS_PROFILE.value:
            con_params['iam'] = True
            con_params['db_user'] = self.db_user
            con_params['profile'] = self.profile
            con_params['region'] = self.region
        return {k: v for k, v in con_params.items() if v is not None}

    def _get_connection(self, database) -> redshift_connector.Connection:
        """Establish a connection to an Amazon Redshift cluster."""
        con = redshift_connector.connect(
            **self._get_connection_params(
                database=database if database else None,
            ),
        )
        con.autocommit = True  # see https://stackoverflow.com/q/22019154
        con.paramstyle = 'pyformat'
        return con

    def _retrieve_tables(self, database) -> list[str]:
        with self._get_connection(database=database).cursor() as cursor:
            cursor.execute(TABLE_QUERY)
            res = cursor.fetchall()
        return [table_name for (table_name,) in res]

    def _retrieve_data(
        self,
        datasource: RedshiftDataSource,
        get_row_count: bool = False,
        offset: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        if get_row_count:
            prepared_query, prepared_query_parameters = SqlQueryHelper.prepare_count_query(
                datasource.query, datasource.parameters
            )
        else:
            prepared_query, prepared_query_parameters = SqlQueryHelper.prepare_limit_query(
                datasource.query, datasource.parameters, offset, limit
            )
        with self._get_connection(database=datasource.database).cursor() as cursor:
            cursor.execute(prepared_query, prepared_query_parameters)
            result: pd.DataFrame = cursor.fetch_dataframe()
            if result is None:
                result = pd.DataFrame()
        return result

    def get_slice(
        self,
        data_source: RedshiftDataSource,
        permissions: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int | None = None,
        get_row_count: bool = False,
    ) -> DataSlice:
        """
        Method to retrieve a part of the data as a pandas dataframe
        and the total size filtered with permissions
        - offset is the index of the starting row
        - limit is the number of pages to retrieve
        Exemple: if offset = 5 and limit = 10 then 10 results are expected from 6th row
        """
        df: pd.DataFrame = self._retrieve_data(data_source, False, offset, limit)
        total_returned_rows = len(df) if df is not None else 0

        run_count_request = get_row_count and SqlQueryHelper.count_query_needed(data_source.query)

        if run_count_request:
            df_count: pd.DataFrame = self._retrieve_data(data_source, True)
            total_rows = (
                df_count.total_rows[0]
                if df_count is not None and len(df_count.total_rows) > 0
                else 0
            )
        else:
            total_rows = total_returned_rows

        return DataSlice(
            df,
            pagination_info=build_pagination_info(
                offset=offset,
                limit=limit,
                total_rows=total_rows,
                retrieved_rows=total_returned_rows,
            ),
        )

    @staticmethod
    def _get_details(index: int, status: bool):
        checks = [
            'Hostname resolved',
            'Port opened',
            'Authenticated',
            'Default Database connection',
        ]
        ok_checks = [(c, True) for i, c in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(c, False) for i, c in enumerate(checks) if i > index]
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

        # Basic db query
        try:
            redshift_connector.connect(
                **self._get_connection_params(database=self.default_database),
            )
        except (Exception, redshift_connector.OperationalError) as e:
            return ConnectorStatus(status=False, details=self._get_details(3, False), error=str(e))

        return ConnectorStatus(status=True, details=self._get_details(3, True), error=None)

    def describe(self, data_source: RedshiftDataSource) -> dict[str, Any]:
        with self._get_connection(database=data_source.database).cursor() as cursor:
            cursor.execute(DESCRIBE_QUERY.format(column=data_source.query.replace(';', '')))
            res = cursor.description
        return {
            col[0].decode('utf-8') if isinstance(col[0], bytes) else col[0]: types_map.get(col[1])
            for col in res
        }

    def _db_table_info_rows(self, database: str) -> list[tuple[str, str, str, str]]:
        with self._get_connection(database).cursor() as cursor:
            """Get rows of (schema, table name, column name, column type)"""
            cursor.execute(
                r"""SELECT "schemaname", "tablename", "column", "type" FROM PG_TABLE_DEF WHERE schemaname = 'public';"""
            )
            return cursor.fetchall()

    def _db_tables_info(self, database: str) -> list[tuple[str, str, str, str, str]]:
        """Get rows of (database, schema, table name, column name, column type)"""
        table_infos = []
        for schema, table_name, column_name, column_type in self._db_table_info_rows(database):
            for row in table_infos[::-1]:
                if row['schema'] == schema and row['name'] == table_name:
                    row['columns'].append({'name': column_name, 'type': column_type})
                    break
            else:
                table_infos.append(
                    {
                        'database': database,
                        'schema': schema,
                        'name': table_name,
                        'type': 'table',
                        'columns': [{'name': column_name, 'type': column_type}],
                    }
                )
        return table_infos

    def get_model(self, db_name: str | None = None) -> list[TableInfo]:
        """Retrieves the database tree structure using current connection"""
        tables_info = []
        dbs = self.available_dbs if db_name is None else [db_name]
        for db in dbs:
            with suppress(redshift_connector.OperationalError, redshift_connector.ProgrammingError):
                tables_info += self._db_tables_info(db)

        return tables_info

    def get_model_with_info(self, db_name: str | None = None) -> tuple[list[TableInfo], dict]:
        """Retrieves the database tree structure using current connection"""
        databases_tree = []
        failed_databases = []
        dbs = self.available_dbs if db_name is None else [db_name]
        for db in dbs:
            try:
                databases_tree += self._list_tables_info(db)
            except (redshift_connector.OperationalError, redshift_connector.ProgrammingError):
                failed_databases.append(db)

        tables_info = DiscoverableConnector.format_db_model(databases_tree)
        metadata = {}
        if failed_databases:
            metadata['info'] = {'Could not reach databases': failed_databases}
        return (tables_info, metadata)

    def _list_db_names(self) -> list[str]:
        with self._get_connection(database=self.default_database).cursor() as cursor:
            # redshift has a weird system db called padb_harvest duplicating the content of 'dev' database
            # https://bit.ly/3GQJCdy, we have to filter it
            cursor.execute(
                """select datname from pg_database where datistemplate = false and datname != 'padb_harvest';"""
            )
            return [db_name for (db_name,) in cursor.fetchall()]

    def _list_tables_info(self, database: str) -> list[tuple]:
        with self._get_cursor(database) as cursor:
            cursor.execute(build_database_model_extraction_query())
            return cursor.fetchall()
