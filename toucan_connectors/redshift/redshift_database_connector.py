import pandas as pd
import redshift_connector
from pydantic import Field, SecretStr
from pydantic.types import constr
from redshift_connector.error import InterfaceError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class RedshiftDataSource(ToucanDataSource):
    table: str = Field(None, description='The name of the data table that you want to ')
    query: constr(min_length=1) = Field(
        description='A string describing a query (CAUTION: Use limit to avoid to retrieve too many datas)',
        widget='sql',
    )


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource

    user: str = Field(..., description='Your login username.')
    password: SecretStr = Field(None, description='Your login password')
    host: str = Field(None, description='IP address or hostname.')
    port: int = Field(..., description='The listening port of your Redshift Database')
    database: str = Field(..., description='The name of the database you want to query')
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )
    cluster_identifier: str = Field(..., description='Name of the cluster')

    def get_connection_params(self):
        con_params = dict(
            database=self.database,
            user=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            connect_timeout=self.connect_timeout,
            cluster_identifier=self.cluster_identifier,
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _get_connection(self):
        """Establish a connection to an Amazon Redshift cluster."""
        return redshift_connector.connect(**self.get_connection_params())

    def _get_cursor(self) -> redshift_connector.Cursor:
        return self._get_connection().cursor()

    def _retrieve_data(self, datasource) -> pd.DataFrame:
        """Get data: tuple from table."""
        with self._get_cursor() as cursor:
            cursor.execute(datasource.query)
            result: pd.DataFrame = cursor.fetch_dataframe()
        return result

    def get_status(self) -> ConnectorStatus:
        try:
            with self._get_connection():
                return ConnectorStatus(status=True, details=None, error=None)
        except InterfaceError as err:
            return ConnectorStatus(status=False, error=err)
