# Basic support for now, may need to grow up :
# undelying lib seems fragile and not very usefull,
# could be tested with a container etc...

from magento.api import API
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class MagentoDataSource(ToucanDataSource):
    resource_path: str
    arguments: list = []


class MagentoConnector(ToucanConnector):
    type = "Magento"
    data_source_model: MagentoDataSource

    url: str
    username: str
    password: str

    def get_df(self, data_source: MagentoDataSource) -> pd.DataFrame:
        with API(self.url, self.username, self.password) as api:
            res = api.call(data_source.resource_path, *data_source.arguments)
            return pd.DataFrame(res)
