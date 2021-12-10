import logging

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr
from pydantic.types import constr

from toucan_connectors.connection_manager import ConnectionManager
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

DATABASE_QUERY = """select datname from pg_database;"""
TABLE_QUERY = """SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public';"""

logger = logging.getLogger(__name__)

redshift_connection_manager = None
if not redshift_connection_manager:
    redshift_connection_manager = ConnectionManager(
        name='redshift', timeout=10, wait=0.2, time_between_clean=10, time_keep_alive=600
    )


class RedshiftDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    table: str = Field(None, description='The name of the data table that you want to ')
    query: constr(min_length=1) = Field(
        description='A string describing a query (CAUTION: Use limit to avoid to retrieve too many datas)',
        widget='sql',
    )

    # @classmethod
    # def get_form(cls, connector: 'RedshiftConnector', current_config):
    #     constraints = {}
    #
    #     with suppress(Exception):
    #         connection = redshift_connector.connect(
    #             **connector.get_connection_params(database=current_config.get('database'))
    #         )
    #         with connection.cursor() as cursor:
    #             cursor.execute(DATABASE_QUERY)
    #             res = cursor.fetchall()
    #             available_dbs = [datname for (datname,) in res]
    #             constraints['database'] = strlist_to_enum('database', available_dbs)
    #
    #             if 'database' in current_config:
    #                 cursor.execute(TABLE_QUERY)
    #                 res = cursor.fetchall()
    #                 available_tables = [table_name for (_, table_name) in res]
    #                 constraints['table'] = strlist_to_enum('table', available_tables, None)
    #
    #         return create_model('FormSchema', **constraints, __base__=cls).schema()


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource

    user: str = Field(..., description='Your login username.')
    password: SecretStr = Field(None, description='Your login password')
    host: str = Field(None, description='IP address or hostname.')
    port: int = Field(..., description='The listening port of your Redshift Database')

    @staticmethod
    def get_redshift_connection_manager():
        return redshift_connection_manager

    def get_connection_params(self, database):
        con_params = dict(
            database=database,
            user=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _get_connection(self, datasource):
        """Establish a connection to an Amazon Redshift cluster."""

        def connect_function():
            return redshift_connector.connect(
                **self.get_connection_params(
                    database=datasource.database if datasource is not None else None
                )
            )

        def alive_function(connection):
            logger.debug('Check Redshift connection alive')
            if hasattr(connection, 'is_closed'):
                print(not connection.is_closed())
                return not connection.is_closed()

        def close_function(connection):
            logger.info('Close Redshift connection')
            if hasattr(connection, 'close'):
                connection.close()

        connection = redshift_connection_manager.get(
            identifier=datasource.database,
            connect_method=connect_function,
            alive_method=alive_function,
            close_method=close_function,
            save=True if datasource.database else False,
        )
        return connection.connect_method

    def _get_cursor(self, datasource) -> redshift_connector.Cursor:
        return self._get_connection(datasource=datasource).cursor()

    def _retrieve_data(self, datasource) -> pd.DataFrame:
        """Get data: tuple from table."""
        with self._get_cursor(datasource=datasource) as cursor:
            cursor.execute(datasource.query)
            result: pd.DataFrame = cursor.fetch_dataframe()
        return result

    # def get_status(self) -> ConnectorStatus:
    #     try:
    #         with self._get_connection():
    #             return ConnectorStatus(status=True, details=None, error=None)
    #     except InterfaceError as err:
    #         return ConnectorStatus(status=False, error=err)
