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


class SnowflakeConnector(ToucanConnector):
    """
    Import data from Snowflake data warehouse.
    """

    data_source_model: SnowflakeDataSource

    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(..., description='Your login password')
    default_warehouse: str = Field(
        ..., description='The default warehouse that shall be used for any data source'
    )
    account: str = Field(
        ...,
        description='The full name of your Snowflake account. '
        'It might require the region and cloud platform where your account is located, '
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info" target="_blank">here</a>.',
    )
    ocsp_response_cache_filename: Path = Field(
        None,
        title='OCSP response cache filename',
        description='The path of the '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#caching-ocsp-responses" target="_blank">OCSP cache file</a>',
    )

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        warehouse = data_source.warehouse
        # Default to default warehouse if not specified in `data_source`
        if self.default_warehouse and not warehouse:
            warehouse = self.default_warehouse

        connection = snowflake.connector.connect(
            user=self.user,
            password=self.password.get_secret_value(),
            account=self.account,
            database=Template(data_source.database).render(),
            warehouse=Template(warehouse).render(),
            ocsp_response_cache_filename=self.ocsp_response_cache_filename,
        )

        # https://docs.snowflake.net/manuals/sql-reference/sql/use-warehouse.html
        connection.cursor().execute(f'USE WAREHOUSE {warehouse}')

        query = nosql_apply_parameters_to_query(data_source.query, data_source.parameters)
        df = pd.read_sql(query, con=connection)

        connection.close()

        return df
