import logging
from contextlib import suppress
from enum import Enum
from time import sleep
from timeit import default_timer as timer
from typing import Dict, List, Optional

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr, create_model
from pydantic.types import constr

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.connection_manager import ConnectionManager
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum

TABLE_QUERY = """SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public';"""

logger = logging.getLogger(__name__)

redshift_connection_manager = None
if not redshift_connection_manager:
    redshift_connection_manager = ConnectionManager(
        name='redshift', timeout=10, wait=0.2, time_between_clean=10, time_keep_alive=10
    )


class AuthenticationMethod(str, Enum):
    DB_CREDENTIAL: str = 'db_cred'
    IAM: str = 'iam'


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
        AuthenticationMethod.DB_CREDENTIAL,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your snowflake data source',
    )
    user: str = Field(..., description='Your login username.')
    password: SecretStr = Field(None, description='Your login password')
    host: str = Field(None, description='IP address or hostname.')
    port: int = Field(..., description='The listening port of your Redshift Database')
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )

    @staticmethod
    def get_redshift_connection_manager() -> ConnectionManager:
        return redshift_connection_manager

    def get_connection_params(self, database) -> Dict:
        con_params = dict(
            database=database,
            user=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            timeout=self.connect_timeout,
        )
        return {k: v for k, v in con_params.items() if v is not None}

    def _build_connection(self, datasource) -> redshift_connector.Connection:
        return redshift_connector.connect(
            **self.get_connection_params(
                database=datasource.database if datasource is not None else None
            )
        )

    def _get_connection(self, datasource) -> redshift_connector.Connection:
        """Establish a connection to an Amazon Redshift cluster."""

        def connect_function() -> redshift_connector.Connection:
            return self._build_connection(datasource)

        def alive_function(connection) -> bool:
            logger.info(f'Alive Redshift connection: {connection}')
            if self.connect_timeout is not None:
                start = timer()
                while self.connect_timeout - start > 0:
                    sleep(1)
                    return True
            else:
                return False

        def close_function(connection) -> None:
            logger.info('Close Redshift connection')
            if hasattr(connection, 'close'):
                connection.close()

        connection: RedshiftConnector = redshift_connection_manager.get(
            identifier=datasource.database,
            connect_method=connect_function,
            alive_method=alive_function,
            close_method=close_function,
            save=True if datasource.database else False,
        )

        connection.paramstyle = 'pyformat'
        return connection.__enter__()

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
            cursor.execute(datasource.query, datasource.parameters or {})
            result: pd.DataFrame = cursor.fetch_dataframe()
        return result

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
        except Exception:
            return ConnectorStatus(status=True, details=self._get_details(2, False), error=None)
