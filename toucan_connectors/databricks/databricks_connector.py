import logging
from typing import List, Optional, Tuple

import pandas as pd
import pyodbc
import requests
from pydantic import Field, SecretStr, constr
from requests.auth import HTTPBasicAuth

from toucan_connectors.common import ClusterStartException, ConnectorStatus, pandas_read_sql
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

logger = logging.getLogger(__name__)


class DatabricksDataSource(ToucanDataSource):
    query: constr(min_length=1) = Field(
        ...,
        description='You can write a query here',
        widget='sql',
    )


class DatabricksConnector(ToucanConnector):
    data_source_model: DatabricksDataSource
    host: str = Field(
        ...,
        description='The listening address of your databricks cluster',
        placeholder='my-databricks-cluster.cloudproviderdatabricks.net',
    )
    port: int = Field(
        ..., description='The listening port of your databricks cluster', placeholder=443
    )
    http_path: str = Field(
        ..., description='Databricks compute resources URL', placeholder='sql/protocolv1/o/xxx/yyy'
    )
    pwd: SecretStr = Field(
        None, description='Your personal access token', placeholder='dapixxxxxxxxxxx'
    )
    ansi: bool = False
    on_demand: bool = Field(
        False, description="If the cluster is auto terminating, wait for it's start before querying"
    )

    def _build_connection_string(self) -> str:
        """For constants see: https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html"""
        connection_params = {
            'Driver': '/opt/simba/spark/lib/64/libsparkodbc_sb64.so',  # This path must be correct when installing
            # ODBC driver
            'Host': '127.0.0.1' if self.host == 'localhost' else self.host,
            'Port': self.port,
            'HTTPPath': self.http_path,
            'ThriftTransport': 2,
            'SSL': 1,
            'AuthMech': 3,
            'UID': 'token',
            'PWD': self.pwd.get_secret_value(),
        }
        return ';'.join(f'{k}={v}' for k, v in connection_params.items() if v is not None)

    @staticmethod
    def _get_details(index: int, status: Optional[bool]) -> List[Tuple[str, bool]]:
        checks = ['Host resolved', 'Port opened', 'Connected to Databricks', 'Authenticated']
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
        try:
            pyodbc.connect(self._build_connection_string(), autocommit=True, ansi=self.ansi)
        except pyodbc.InterfaceError as e:
            details = self._get_details(3, False)
            return ConnectorStatus(status=False, details=details, error=e.args[0])
        except pyodbc.Error as e:
            return ConnectorStatus(
                status=False, details=self._get_details(2, False), error=e.args[0]
            )
        return ConnectorStatus(status=True, details=self._get_details(3, True), error=None)

    def get_cluster_state(self) -> bool:
        endpoint = f'https://{self.host}/api/2.0/clusters/get'
        auth = HTTPBasicAuth('token', self.pwd.get_secret_value())
        data = {'cluster_id': self.http_path.split('/')[-1]}
        return requests.get(endpoint, auth=auth, json=data).json().get('state')

    def start_cluster(self) -> None:
        endpoint = f'https://{self.host}/api/2.0/clusters/start'
        auth = HTTPBasicAuth('token', self.pwd.get_secret_value())
        data = {'cluster_id': self.http_path.split('/')[-1]}
        resp = requests.post(endpoint, auth=auth, json=data)
        if resp.status_code == 200:
            logger.info('Databricks cluster started')
        else:
            message = resp.json().get('message', 'Failed to start Databricks cluster')
            logger.error(f'Error while starting cluster: {message}')
            raise ClusterStartException(f'failed to start cluster: {message}')

    def _retrieve_data(self, data_source: DatabricksDataSource) -> pd.DataFrame:
        """
        The connector can face a shutdown cluster and must wait it to be started before querying.
        Try to trigger the query, if we receive an error wait for cluster to start then try again
        """
        query_params = data_source.parameters or {}
        connection = pyodbc.connect(
            self._build_connection_string(), autocommit=True, ansi=self.ansi
        )
        result = pandas_read_sql(
            data_source.query,
            con=connection,
            params=query_params,
            convert_to_qmark=True,
        )
        connection.close()
        return result
