from os import path

import pandas as pd
import snowflake.connector
from pydantic import Field, SecretStr, constr

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


class SnowflakeConnector(ToucanConnector):
    """
    Import data from Snowflake data warehouse.
    """

    data_source_model: SnowflakeDataSource

    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(..., description='Your login password')
    account: str = Field(
        ...,
        description='The full name of your Snowflake account. '
        'It might require the region and cloud platform where your account is located, '
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info">here</a>.',
    )
    ocsp_response_cache_filename: Path = Field(
        None,
        title='OCSP response cache filename',
        description='The path of the '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#caching-ocsp-responses">OCSP cache file</a>',
    )

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        connection = snowflake.connector.connect(
            user=self.user,
            password=self.password.get_secret_value(),
            account=self.account,
            database=data_source.database,
            warehouse=data_source.warehouse,
            ocsp_response_cache_filename=self.ocsp_response_cache_filename,
        )

        # https://docs.snowflake.net/manuals/sql-reference/sql/use-warehouse.html
        connection.cursor().execute(f'USE WAREHOUSE {data_source.warehouse}')

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
