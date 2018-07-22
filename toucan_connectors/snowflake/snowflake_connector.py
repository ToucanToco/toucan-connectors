from os import path
import pandas as pd
from pydantic.types import constr
import snowflake.connector

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Path(str):
    @classmethod
    def get_validators(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not path.exists(v):
            raise ValueError(f'path does not exists: v')
        return v


class SnowflakeDataSource(ToucanDataSource):
    database: str = None
    warehouse: str = None
    query: constr(min_length=1)


class SnowflakeConnector(ToucanConnector):
    """
    Import data from Snowflake data warehouse.
    """
    type = 'Snowflake'
    data_source_model: SnowflakeDataSource

    user: str
    password: str
    account: str
    ocsp_response_cache_filename: Path = None

    def get_df(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        connection = snowflake.connector.connect(
            user=self.user, password=self.password, account=self.account,
            database=data_source.database, warehouse=data_source.warehouse,
            ocsp_response_cache_filename=self.ocsp_response_cache_filename)

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
