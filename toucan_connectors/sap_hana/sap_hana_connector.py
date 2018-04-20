import pandas as pd
import pyhdb
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class SapHanaDataSource(ToucanDataSource):
    query: constr(min_length=1)


class SapHanaConnector(ToucanConnector):
    """
    Import data from Sap Hana.
    """
    type = 'SapHana'
    data_source_model: SapHanaDataSource

    host: str
    port: str
    user: str
    password: str

    def get_df(self, data_source):
        connection = pyhdb.connect(self.host, self.port, self.user, self.password)

        df = pd.read_sql(data_source.query, con=connection)

        connection.close()

        return df
