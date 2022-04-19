from time import sleep
from typing import Optional


import pandas as pd
import pyodbc
from pydantic import Field, SecretStr, constr

from toucan_connectors.common import ConnectorStatus, pandas_read_sql
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class DatabricksDataSource(ToucanDataSource):
    database: str = Field(
        None,
        description='The name of the database you want to query. '
        "By default SQL Server selects the user's default database",
    )
    table: constr(min_length=1) = Field(
        None,
        description='The name of the data table that you want to '
        'get (equivalent to "SELECT * FROM '
        'your_table")',
    )
    query: constr(min_length=1) = Field(
        None,
        description='You can write a custom query against your '
        'database here. It will take precedence over '
        'the "table" parameter above',
        widget='sql',
    )


class DataBricksConnectionError(Exception):
    """ """


class DatabricksConnector(ToucanConnector):
    data_source_model: DatabricksDataSource
    Host: str = Field(
        ...,
        description='The listening address of your databricks cluster',
        placeholder='my-databricks-cluster.cloudproviderdatabricks.net',
    )
    Port: int = Field(
        ..., description='The listening port of your databricks cluster', placeholder=443
    )
    HTTPPath: str = Field(
        ..., description='Databricks compute resources URL', placeholder='sql/protocolv1/o/xxx/yyy'
    )
    UID: str = Field(None, description='User ID if not set value is token')
    PWD: SecretStr = Field(
        ..., description='Your personal access token', placeholder='dapixxxxxxxxxxx'
    )
    Ansi: bool = False
    connect_timeout: int = None
    cluster_start_timeout: int = Field(
        30, description='The time to wait for cluster to be started before querying'
    )

    def _build_connection_string(self, datasource: Optional[DatabricksDataSource] = None) -> str:
        """For constants see: https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html"""
        connection_params = {
            'Driver': '/opt/simba/spark/lib/64/libsparkodbc_sb64.so',  # This path must be correct when installing
            # ODBC driver
            'Host': '127.0.0.0.1' if self.Host == 'localhost' else self.Host,
            'Port': self.Port,
            'HTTPPath': self.HTTPPath,
            'ThriftTransport': 2,
            'SSL': 1,
            'AuthMech': 3,
            'UID': self.UID if self.UID else 'token',
            'PWD': self.PWD,
            'database': datasource.database if datasource else None,
            'Autocommit': True,
            'Ansi': self.Ansi,
            'connect_timeout': self.connect_timeout,
        }
        return ';'.join(f'{k}={v}' for k, v in connection_params.items() if v is not None)

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = ['Host resolved', 'Port opened', 'Connected to Databricks', 'Authenticated']
        ok_checks = [(c, True) for i, c in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(c, None) for i, c in enumerate(checks) if i > index]
        return ok_checks + [new_check] + not_validated_checks

    def get_status(self) -> ConnectorStatus:
        # Check hostname
        try:
            self.check_hostname(self.Host)
        except Exception as e:
            return ConnectorStatus(status=False, details=self._get_details(0, False), error=str(e))
        # Check port
        try:
            self.check_port(self.Host, self.Port)
        except Exception as e:
            return ConnectorStatus(status=False, details=self._get_details(1, False), error=str(e))
        try:
            pyodbc.connect(self._build_connection_string())
        except (pyodbc.InterfaceError, Exception) as e:
            error_code = e.args[0]
            if 'Authentication/authorization error occured' in error_code:
                return ConnectorStatus(
                    status=False, details=self._get_details(3, False), error=e.args[0]
                )
            else:
                return ConnectorStatus(
                    status=False, details=self._get_details(2, False), error=e.args[0]
                )
        return ConnectorStatus(status=True, details=self._get_details(3, True), error=None)

    def _cluster_started(self, datasource: DatabricksDataSource) -> bool:
        """First check that the connection params are valid, then fire a dummy query to start the cluster
        and wait a given time for the answer before returning the check's outcome"""
        checks = self.get_status()
        if checks.status:
            try:
                with pyodbc.connect(self._build_connection_string(datasource)) as connection:
                    if connection.cursor.execute('select 1;').fetchone():
                        return True
                    else:
                        sleep(self.cluster_start_timeout)
                        if connection.cursor.execute('select 1;').fetchone():
                            return True
                        else:
                            return False
            except Exception:
                raise DataBricksConnectionError

    def _retrieve_data(self, data_source: DatabricksDataSource) -> pd.DataFrame:
        """
        The connector can face a shutdown cluster and must wait it to be started before querying. To do so, call
        _is_cluster_started, then fire the given query
        """
        with pyodbc.connect(self._build_connection_string(data_source)) as connection:
            if self._cluster_started(data_source):
                query_params = data_source.parameters or {}
                return pandas_read_sql(
                    data_source.query,
                    con=connection,
                    params=query_params,
                    convert_to_qmark=True,
                    render_user=True,
                )
            else:
                raise DataBricksConnectionError
