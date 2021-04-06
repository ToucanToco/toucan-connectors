import os
import pathlib
from typing import Dict, List, Optional

import pandas as pd
import snowflake.connector
from pydantic import Field, constr
from snowflake.connector import DictCursor

from toucan_connectors.common import convert_to_qmark_paramstyle
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

AUTHORIZATION_URL = (
    'https://toucantocopartner.west-europe.azure.snowflakecomputing.com/oauth/authorize'
)
SCOPE = 'refresh_token'
TOKEN_URL = 'https://toucantocopartner.west-europe.azure.snowflakecomputing.com/oauth/token-request'


class SnowflakeIdsDataSource(ToucanDataSource):
    database: str = Field(None, description='The name of the database you want to query')
    warehouse: str = Field(None, description='The name of the warehouse you want to query')
    query: constr(min_length=1) = Field(
        None, description='You can write your SQL query here', widget='sql'
    )

    @classmethod
    def _get_databases(cls, connector: 'SnowflakeIdsConnector'):
        with connector.connect() as connection:
            return [
                db['name']
                # Fetch rows as dicts with column names as keys
                for db in connection.cursor(DictCursor).execute('SHOW DATABASES').fetchall()
                if 'name' in db
            ]


class SnowflakeIdsConnector(ToucanConnector):
    """
    Import data from Snowflake data warehouse.
    """

    data_source_model: SnowflakeIdsDataSource
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]
    account: str = Field(None)

    @staticmethod
    def get_connector_secrets_form() -> ConnectorSecretsForm:
        return ConnectorSecretsForm(
            documentation_md=(pathlib.Path(os.path.dirname(__file__)) / 'doc.md').read_text(),
            secrets_schema=OAuth2ConnectorConfig.schema(),
        )

    def __init__(self, *args, **kwargs):
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=SCOPE,
            token_url=TOKEN_URL,
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
            secrets_keeper=kwargs['secrets_keeper'],
        )

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def connect(self, **kwargs) -> snowflake.connector.SnowflakeConnection:
        # This needs to be set before we connect
        snowflake.connector.paramstyle = 'qmark'
        connection_params = {
            'account': self.account,
            'authenticator': 'oauth',
            'application': 'ToucanToco',
            'token': self.get_access_token(),
        }

        return snowflake.connector.connect(**connection_params, **kwargs)

    def _get_warehouses(self) -> List[str]:
        with self.connect() as connection:
            return [
                warehouse['name']
                for warehouse in connection.cursor(DictCursor).execute('SHOW WAREHOUSES').fetchall()
                if 'name' in warehouse
            ]

    def _execute_query(self, cursor, query: str, query_parameters: Dict):
        """Executes `query` against Snowflake's client and retrieves the
        results.

        Args:
            cursor (SnowflakeCursor): A snowflake database cursor
            query (str): The query that will be executed against a database
            query_parameters (Dict): The eventual parameters that will be applied to `query`

        Returns: A pandas DataFrame
        """
        # Prevent error with dict and array values in the parameter object
        converted_query, ordered_values = convert_to_qmark_paramstyle(query, query_parameters)
        query_res = cursor.execute(converted_query, ordered_values)
        # This IDS connector is read only, hence we only use fetch_pandas_all()
        return query_res.fetch_pandas_all()

    def _retrieve_data(self, data_source: SnowflakeIdsDataSource) -> pd.DataFrame:

        connection = self.connect(
            database=data_source.database,
            warehouse=data_source.warehouse,
        )
        with connection.cursor(DictCursor) as curs:
            df = self._execute_query(curs, data_source.query, data_source.parameters)

        return df
