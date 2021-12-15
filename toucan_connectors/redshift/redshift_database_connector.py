import logging
import time
from contextlib import suppress
from enum import Enum
from threading import Thread
from typing import Any, Dict, List, Optional

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr, create_model, root_validator
from pydantic.types import constr

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.connection_manager import ConnectionManager
from toucan_connectors.toucan_connector import (
    DataSlice,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

TABLE_QUERY = """SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public';"""

logger = logging.getLogger(__name__)

redshift_connection_manager = None
if not redshift_connection_manager:
    redshift_connection_manager = ConnectionManager(
        name='redshift', timeout=25, wait=0.2, time_between_clean=10, time_keep_alive=600
    )


class AuthenticationMethod(str, Enum):
    DB_CREDENTIALS: str = 'db_credentials'
    AWS_CREDENTIALS: str = 'aws_credentials'
    AWS_PROFILE: str = 'aws_profile'


class AuthenticationMethodError(str, Enum):
    DB_CREDENTIALS: str = f'User & Password are required for {AuthenticationMethod.DB_CREDENTIALS}'
    AWS_CREDENTIALS: str = f'AccessKeyId, SecretAccessKey, SessionToken & db_user are required for {AuthenticationMethod.AWS_CREDENTIALS}'
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
    def get_form(cls, connector: 'RedshiftConnector', current_config):
        constraints = {}
        with suppress(Exception):
            if 'database' in current_config:
                available_tables = connector._retrieve_tables(current_config['database'])
                constraints['table'] = strlist_to_enum('table', available_tables, None)
        return create_model('FormSchema', **constraints, __base__=cls).schema()


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource
    authentication_method: AuthenticationMethod = Field(
        None,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your snowflake data source',
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
            ordered_keys = [
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
            schema['properties'] = {k: schema['properties'][k] for k in ordered_keys}

    @root_validator
    def check_requirements(cls, values):
        mode = values.get('authentication_method')
        if mode == AuthenticationMethod.DB_CREDENTIALS:
            user, password = values.get('user'), values.get('password')
            if user is None or password is None or password.get_secret_value() is None:
                raise ValueError(AuthenticationMethodError.DB_CREDENTIALS)
        elif mode == AuthenticationMethod.AWS_CREDENTIALS:
            access_key_id, secret_access_key, session_token, db_user = (
                values.get('access_key_id'),
                values.get('secret_access_key'),
                values.get('session_token'),
                values.get('db_user'),
            )
            if (
                access_key_id is None
                or secret_access_key is None
                or secret_access_key.get_secret_value() is None
                or session_token is None
                or db_user is None
            ):
                raise ValueError(AuthenticationMethodError.AWS_CREDENTIALS)
        elif mode == AuthenticationMethod.AWS_PROFILE:
            profile, db_user = (values.get('profile'), values.get('db_user'))
            if profile is None or db_user is None:
                raise ValueError(AuthenticationMethodError.AWS_PROFILE)
        else:
            raise ValueError(AuthenticationMethodError.UNKNOWN)
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
        if self.authentication_method == AuthenticationMethod.DB_CREDENTIALS:
            con_params['user'] = self.user
            con_params['password'] = self.password.get_secret_value() if self.password else None
        elif self.authentication_method == AuthenticationMethod.AWS_CREDENTIALS:
            con_params['iam'] = True
            con_params['db_user'] = self.db_user
            con_params['access_key_id'] = self.access_key_id
            con_params['secret_access_key'] = (
                self.secret_access_key.get_secret_value() if self.secret_access_key else None
            )
            con_params['session_token'] = self.session_token
            con_params['region'] = self.region
        elif self.authentication_method == AuthenticationMethod.AWS_PROFILE:
            con_params['iam'] = True
            con_params['db_user'] = self.db_user
            con_params['profile'] = self.profile
            con_params['region'] = self.region
        return {k: v for k, v in con_params.items() if v is not None}

    def _build_connection(self, datasource) -> redshift_connector.Connection:
        connection = redshift_connector.connect(
            **self._get_connection_params(
                database=datasource.database if datasource is not None else None,
            )
        )
        return connection

    def _get_connection(self, datasource) -> redshift_connector.Connection:
        """Establish a connection to an Amazon Redshift cluster."""

        def connect_function() -> redshift_connector.Connection:
            return self._build_connection(datasource)

        def alive_function(connection) -> bool:
            logger.info(f'Alive Redshift connection: {connection}')
            return self._is_alive

        def close_function(connection) -> None:
            logger.info('Close Redshift connection')
            if hasattr(connection, 'close'):
                connection.close()

        connection: RedshiftConnector = redshift_connection_manager.get(
            identifier=f'{self.cluster_identifier}_{datasource.database}',
            connect_method=connect_function,
            alive_method=alive_function,
            close_method=close_function,
            save=True if datasource.database else False,
        )
        connection.paramstyle = 'pyformat'
        if self.connect_timeout is not None:
            self._start_timer_alive()
        return connection.__enter__()

    def _start_timer_alive(self):
        timerThread = Thread(target=self._set_alive_done)
        timerThread.daemon = True
        timerThread.start()

    def _set_alive_done(self):
        time.sleep(self.connect_timeout)
        self._is_alive = False

    def _get_cursor(self, datasource) -> redshift_connector.Cursor:
        return self._get_connection(datasource=datasource).cursor()

    def _retrieve_tables(self, datasource) -> List[str]:
        with self._get_cursor(datasource=datasource) as cursor:
            cursor.execute(TABLE_QUERY)
            res = cursor.fetchall()
        return [table_name for (table_name,) in res]

    def _retrieve_data(self, datasource) -> pd.DataFrame:
        """Get data: tuple from table."""
        with self._get_cursor(datasource=datasource) as cursor:
            cursor.execute(datasource.query)
            result: pd.DataFrame = cursor.fetch_dataframe()
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
        df = self._retrieve_data(data_source)
        if limit is not None:
            return DataSlice(df[offset : offset + limit], len(df))
        else:
            return DataSlice(df[offset:], len(df))

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
        # Check connection
        try:
            with self._build_connection(None):
                return ConnectorStatus(status=True, details=self._get_details(2, True), error=None)
        except redshift_connector.error.ProgrammingError as ex:
            # Use to validate if the issue is "only" an issue with database (set after with datasource)
            if f"'S': 'FATAL', 'C': '3D000', 'M': 'database {self.user} does not exist'" in str(ex):
                return ConnectorStatus(
                    status=True, details=self._get_details(2, True), error=str(ex)
                )
            # Same check for IAM mode
            elif (
                f"'S': 'FATAL', 'C': '3D000', 'M': 'database \"IAM:{self.db_user}\" does not exist'"
                in str(ex)
            ):
                return ConnectorStatus(
                    status=True, details=self._get_details(2, True), error=str(ex)
                )
            else:
                return ConnectorStatus(
                    status=False, details=self._get_details(2, False), error=str(ex)
                )
        except Exception as ex:
            return ConnectorStatus(status=False, details=self._get_details(2, False), error=str(ex))
