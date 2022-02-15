import logging
import time
from contextlib import contextmanager, suppress
from enum import Enum
from threading import Thread
from typing import Any, Dict, List, Optional

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr, create_model, root_validator
from pydantic.types import constr

from toucan_connectors.common import ConnectorStatus, format_db_model
from toucan_connectors.connection_manager import ConnectionManager
from toucan_connectors.redshift.utils import (
    aggregate_columns,
    build_database_model_extraction_query,
    create_columns_query,
    merge_columns_and_tables,
    types_map,
)
from toucan_connectors.sql_query_helper import SqlQueryHelper
from toucan_connectors.toucan_connector import (
    DataSlice,
    DataStats,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

TABLE_QUERY = """SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public';"""

DESCRIBE_QUERY = """SELECT * FROM ({column}) AS q LIMIT 0;"""

ORDERED_KEYS = [
    'type',
    'name',
    'host',
    'port',
    'cluster_identifier',
    'db_user',
    'connect_timeout',
    'authentication_method',
    'user',
    'password',
    'access_key_id',
    'secret_access_key',
    'session_token',
    'profile',
    'region',
]

logger = logging.getLogger(__name__)

redshift_connection_manager = None
if not redshift_connection_manager:
    redshift_connection_manager = ConnectionManager(
        name='redshift', timeout=25, wait=0.2, time_between_clean=10, time_keep_alive=60
    )


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
    database: str = Field(..., description='The name of the database you want to query')
    query: constr(min_length=1) = Field(
        None,
        description='You can write a custom query against your '
        'database here. It will take precedence over '
        'the table parameter',
        widget='sql',
    )
    query_object: Dict = Field(
        None,
        description='An object describing a simple select query, this field is used internally',
        **{'ui.hidden': True},
    )
    table: constr(min_length=1) = Field(
        None,
        description='The name of the data table that you want to '
        'get (equivalent to "SELECT * FROM '
        'your_table")',
    )
    language: str = Field('sql', **{'ui.hidden': True})

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get('query')
        table = data.get('table')
        if query is None and table is None:
            self.query = TABLE_QUERY
        elif query is None and table is not None:
            self.query = f'select * from {table};'

    @classmethod
    def get_form(cls, connector: 'RedshiftConnector', current_config):
        constraints = {}
        with suppress(Exception):
            if 'database' in current_config:
                ds = RedshiftDataSource(
                    domain='Redshift', name='redshift', database=current_config['database']
                )
                available_tables = connector._retrieve_tables(database=ds.database)
                constraints['table'] = strlist_to_enum('table', available_tables, None)
        return create_model('FormSchema', **constraints, __base__=cls).schema()


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource
    authentication_method: AuthenticationMethod = Field(
        None,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your redshift data source',
        **{'ui': {'checkbox': False}},
    )
    host: str = Field(..., description='IP address or hostname.')
    port: int = Field(..., description='The listening port of your Redshift Database')
    cluster_identifier: str = Field(..., description='The cluster of redshift.')
    connect_timeout: Optional[int] = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )

    user: Optional[str] = Field(None, description='Your login username.')
    password: Optional[SecretStr] = Field(None, description='Your login password')

    db_user: Optional[str] = Field(None, description='The user of the database')
    access_key_id: Optional[str] = Field(None, description='The access key id of your aws account.')
    secret_access_key: Optional[SecretStr] = Field(
        None, description='The secret access key of your aws account.'
    )
    session_token: Optional[str] = Field(None, description='Your session token')
    profile: Optional[str] = Field(None, description='AWS profile')
    region: Optional[str] = Field(
        None, description='The region in which there is your aws account.'
    )
    _is_alive: bool = True

    class Config:
        underscore_attrs_are_private = True

        @staticmethod
        def schema_extra(schema: Dict[str, Any]) -> None:
            schema['properties'] = {k: schema['properties'][k] for k in ORDERED_KEYS}

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

    @staticmethod
    def get_redshift_connection_manager() -> ConnectionManager:
        return redshift_connection_manager

    def _get_connection_params(self, database) -> Dict:
        con_params = dict(
            database=database,
            host=self.host,
            port=self.port,
            timeout=self.connect_timeout,
            cluster_identifier=self.cluster_identifier,
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

        def connect_function() -> redshift_connector.Connection:
            con = redshift_connector.connect(
                **self._get_connection_params(
                    database=database if database else None,
                ),
            )
            con.autocommit = True  # see https://stackoverflow.com/q/22019154
            con.paramstyle = 'pyformat'
            return con

        def alive_function(connection) -> bool:
            logger.info(f'Alive Redshift connection: {connection}')
            return self._is_alive

        def close_function(connection) -> None:
            logger.info('Close Redshift connection')
            if not self._is_alive:
                return connection.close()

        connection: redshift_connector.Connection = redshift_connection_manager.get(
            identifier=f'{self.get_identifier()}{database}{self.cluster_identifier}',
            connect_method=connect_function,
            alive_method=alive_function,
            close_method=close_function,
            save=True if database else False,
        )
        if self.connect_timeout is not None:
            t = Thread(target=self.sleeper)
            t.start()
        return connection

    def sleeper(self):
        time.sleep(self.connect_timeout)
        self._is_alive = False

    @contextmanager
    def _get_cursor(self, database) -> redshift_connector.Cursor:
        with self._get_connection(database=database) as conn, conn.cursor() as cursor:
            yield cursor

    def _retrieve_tables(self, database) -> List[str]:
        with self._get_cursor(database=database) as cursor:
            cursor.execute(TABLE_QUERY)
            res = cursor.fetchall()
        return [table_name for (table_name,) in res]

    def _retrieve_data(
        self,
        datasource: RedshiftDataSource,
        get_row_count: bool = False,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        if get_row_count:
            prepared_query, prepared_query_parameters = SqlQueryHelper.prepare_count_query(
                datasource.query, datasource.parameters
            )
        else:
            prepared_query, prepared_query_parameters = SqlQueryHelper.prepare_limit_query(
                datasource.query, datasource.parameters, offset, limit
            )
        with self._get_cursor(database=datasource.database) as cursor:
            cursor.execute(prepared_query, prepared_query_parameters)
            result: pd.DataFrame = cursor.fetch_dataframe()
            if result is None:
                result = pd.DataFrame()
        return result

    def get_slice(
        self,
        data_source: RedshiftDataSource,
        permissions: Optional[dict] = None,
        offset: int = 0,
        limit: Optional[int] = None,
        get_row_count: Optional[bool] = False,
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
            stats=DataStats(total_returned_rows=total_returned_rows, total_rows=total_rows),
        )

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = ['Hostname resolved', 'Port opened', 'Authenticated']
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
        return ConnectorStatus(status=True, details=self._get_details(2, True), error=None)

    def describe(self, data_source: RedshiftDataSource) -> Dict:
        with self._get_cursor(database=data_source.database) as cursor:
            cursor.execute(DESCRIBE_QUERY.format(column=data_source.query.replace(';', '')))
            res = cursor.description
        return {
            col[0].decode('utf-8') if isinstance(col[0], bytes) else col[0]: types_map.get(col[1])
            for col in res
        }

    def get_model(self, db_name: str):
        with self._get_cursor(database=db_name) as cursor:
            # redshift has a weird system db called padb_harvest duplicating the content of 'dev' database
            # https://bit.ly/3GQJCdy, we have to filter it
            cursor.execute(
                """select datname from pg_database where datistemplate = false and datname != 'padb_harvest';"""
            )
            available_dbs = [db_name for (db_name,) in cursor.fetchall()]
            databases_tree = []
        for db in available_dbs:
            with self._get_cursor(database=db_name) as cursor:
                cursor.execute(create_columns_query(db))
                cols = aggregate_columns(cursor.fetch_dataframe())
                cursor.execute(build_database_model_extraction_query())
                databases_tree += merge_columns_and_tables(cols, cursor.fetch_dataframe())
        return format_db_model(databases_tree)
