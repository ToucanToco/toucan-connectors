import pandas as pd
from pydantic.types import constr
import snowflake.connector

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class SnowflakeDataSource(ToucanDataSource):
    database: str = None
    warehouse: str = None
    query: constr(min_length=1)


class SnowflakeConnector(ToucanConnector):
    type = 'Snowflake'
    data_source_model: SnowflakeDataSource

    user: str
    password: str
    account: str
    ocsp_response_cache_filename: str = None

    def get_df(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        connection = snowflake.connector.connect(
            user=self.user, password=self.password, account=self.account,
            database=data_source.database, warehouse=data_source.warehouse,
            ocsp_response_cache_filename=self.ocsp_response_cache_filename)

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
