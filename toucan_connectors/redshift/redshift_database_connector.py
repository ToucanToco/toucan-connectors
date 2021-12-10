from contextlib import suppress

import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr, create_model
from pydantic.types import constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum

DATABASE_QUERY = """select datname from pg_database;"""


class RedshiftDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    table: str = Field(None, description='The name of the data table that you want to ')
    query: constr(min_length=1) = Field(
        description='A string describing a query (CAUTION: Use limit to avoid to retrieve too many datas)',
        widget='sql',
    )

    @classmethod
    def get_form(cls, connector: 'RedshiftConnector', current_config):
        constraints = {}

        with suppress(Exception):
            connection = redshift_connector.connect(
                **connector.get_connection_params(database=current_config.get('database'))
            )
            with connection.cursor() as cursor:
                cursor.execute(DATABASE_QUERY)
                res = cursor.fetchall()
                available_dbs = [datname for (datname,) in res]
                constraints['database'] = strlist_to_enum('database', available_dbs)
            return create_model('FormSchema', **constraints, __base__=cls).schema()


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource

    user: str = Field(..., description='Your login username.')
    password: SecretStr = Field(None, description='Your login password')
    host: str = Field(None, description='IP address or hostname.')
    port: int = Field(..., description='The listening port of your Redshift Database')

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
        return redshift_connector.connect(
            **self.get_connection_params(
                database=datasource.database if datasource is not None else None
            )
        )

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
