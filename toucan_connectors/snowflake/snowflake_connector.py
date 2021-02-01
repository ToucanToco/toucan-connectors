from enum import Enum
from os import path

import pandas as pd
import snowflake.connector
from jinja2 import Template
from pydantic import Field, SecretStr, constr

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Path(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not path.exists(v):
            raise ValueError(f'path does not exists: {v}')
        return v


class SnowflakeDataSource(ToucanDataSource):
    database: str = Field(None, description='The name of the database you want to query')
    warehouse: str = Field(None, description='The name of the warehouse you want to query')
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )

    @classmethod
    def get_form(cls, connector: 'SnowflakeConnector', current_config):
        res = cls.schema()
        res['properties']['warehouse']['default'] = connector.default_warehouse
        return res


class AuthenticationMethod(str, Enum):
    PLAIN: str = 'snowflake'
    OAUTH: str = 'oauth'


class SnowflakeConnector(ToucanConnector):
    """
    Import data from Snowflake data warehouse.
    """

    data_source_model: SnowflakeDataSource

    authentication_method: AuthenticationMethod = Field(
        ...,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your snowflake data source',
    )

    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    oauth_token: str = Field(None, description='Your oauth token')
    account: str = Field(
        ...,
        description='The full name of your Snowflake account. '
        'It might require the region and cloud platform where your account is located, '
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info" target="_blank">here</a>.',
    )

    default_warehouse: str = Field(
        ..., description='The default warehouse that shall be used for any data source'
    )
    ocsp_response_cache_filename: Path = Field(
        None,
        title='OCSP response cache filename',
        description='The path of the '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#caching-ocsp-responses" target="_blank">OCSP cache file</a>',
    )

    def get_connection_params(self):
        res = {
            'user': self.user,
            'account': self.account,
            'authenticator': self.authentication_method,
        }

        if self.authentication_method == AuthenticationMethod.PLAIN and self.password:
            res['password'] = self.password.get_secret_value()

        if self.authentication_method == AuthenticationMethod.OAUTH:
            res['token'] = Template(self.oauth_token).render()

        return res

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        warehouse = data_source.warehouse
        # Default to default warehouse if not specified in `data_source`
        if self.default_warehouse and not warehouse:
            warehouse = self.default_warehouse

        connection_params = self.get_connection_params()

        connection = snowflake.connector.connect(
            database=Template(data_source.database).render(),
            warehouse=Template(warehouse).render(),
            ocsp_response_cache_filename=self.ocsp_response_cache_filename,
            **connection_params,
        )

        # https://docs.snowflake.net/manuals/sql-reference/sql/use-warehouse.html
        connection.cursor().execute(f'USE WAREHOUSE {warehouse}')

        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        df = pd.read_sql(query, con=connection)

        connection.close()

        return df
