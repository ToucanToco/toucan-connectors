import pandas as pd
from odata import ODataService

from toucan_connectors.common import Auth
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class ODataDataSource(ToucanDataSource):
    entity: str
    query: dict


class ODataConnector(ToucanConnector):
    type = "OData"
    data_source_model: ODataDataSource

    baseroute: str
    auth: Auth = None

    def get_df(self, data_source: ODataDataSource) -> pd.DataFrame:

        if self.auth:
            session = self.auth.get_session()
        else:
            session = None

        service = ODataService(self.baseroute, reflect_entities=True, session=session)
        entities = service.entities[data_source.entity]
        data = service.query(entities).raw(data_source.query)
        return pd.DataFrame(data)
