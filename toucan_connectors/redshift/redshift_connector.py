from enum import Enum

import pandas as pd
import redshift_connector
from pydantic import SecretStr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class AuthenticationMethod(str, Enum):
    DB: str = 'Database credentials'
    IAM: str = 'IAM Credentials'
    PROFILE: str = 'Authentication Profile'
    IDP: str = 'Identity Provider'


class RedshiftDataSource(ToucanDataSource):

    database: str
    query: str
    table: str


class RedshiftConnector(ToucanConnector):
    data_source_model: RedshiftDataSource

    dbname: str
    user: str
    password: SecretStr
    host: str
    port: int
    connect_timeout: int

    def define_params_for_auth_method(self):
        params = dict(
            DB=['dbname', 'user', 'password', 'host', 'port', 'connect_timeout'],
            IAM=['iam', 'database', 'db_user', 'password', 'user', 'cluster_identifier', 'profile'],
            PROFILE=['host', 'region', 'cluster_identifier', 'db_name'],
            IDP=[
                'iam',
                'database',
                'cluster_identifier',
                'credentials_provider',
                'user',
                'password',
                'idp_host',
            ],
        )
        return params

    def get_connection_params(self, *, database=None):
        con_params = dict(
            dbname=database,
            user=self.user,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            connect_timeout=self.connect_timeout,
        )
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def get_cursor_connection(self, data_source: RedshiftDataSource):
        connection = redshift_connector.connect(
            **self.get_connection_params(database=data_source.database)
        )
        cursor: redshift_connector.Cursor = connection.cursor()
        return cursor

    def _retrieve_data(self, data_source: RedshiftDataSource, query) -> pd.DataFrame:
        """Get data: tuple from table."""
        with redshift_connector.connect(
            **self.get_connection_params(database=data_source.database)
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
        return result

    def _validate_connection(con: redshift_connector.Connection) -> None:
        pass
