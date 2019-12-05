import pandas as pd
from pyhive import hive

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class HiveDataSource(ToucanDataSource):
    database: str = 'default'
    query: str


class HiveConnector(ToucanConnector):
    data_source_model: HiveDataSource

    host: str
    port: int = 10000
    auth: str = 'NONE'
    configuration: dict = None
    kerberos_service_name: str = None
    username: str = None
    password: str = None

    def _retrieve_data(self, data_source: HiveDataSource) -> pd.DataFrame:
        cursor = hive.connect(
            host=self.host,
            port=self.port,
            username=self.username,
            database=data_source.database,
            auth=self.auth,
            configuration=self.configuration,
            kerberos_service_name=self.kerberos_service_name,
            password=self.password,
        ).cursor()
        cursor.execute(data_source.query, parameters=data_source.parameters)
        columns = [metadata[0] for metadata in cursor.description]
        return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
