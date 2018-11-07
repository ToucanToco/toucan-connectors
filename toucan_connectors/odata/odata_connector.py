import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from odata import ODataService


class ODataDataSource(ToucanDataSource):
    entity: str
    query: dict


class ODataConnector(ToucanConnector):
    type = "OData"
    data_source_model: ODataDataSource

    username: str
    password: str
    url: str

    def get_df(self, data_source: ODataDataSource) -> pd.DataFrame:
        Service = ODataService(self.url, reflect_entities=True)
        Entities = Service.entities[data_source.entity]
        data = Service.query(Entities).raw(data_source.query)
        return pd.DataFrame(data)
