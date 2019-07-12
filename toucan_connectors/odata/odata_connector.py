import pandas as pd
from odata import ODataService
from odata.metadata import MetaData

from toucan_connectors.auth import Auth
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


# monkey patch MetaData's __init__
# (cf. https://github.com/tuomur/python-odata/issues/22)
def metadata_init_patched(self, service):
    self._original_init(service)
    self.url = service.url.rstrip('/') + '/$metadata'


MetaData._original_init = MetaData.__init__
MetaData.__init__ = metadata_init_patched


class ODataDataSource(ToucanDataSource):
    entity: str
    query: dict


class ODataConnector(ToucanConnector):
    data_source_model: ODataDataSource

    baseroute: str
    auth: Auth = None

    def _retrieve_data(self, data_source: ODataDataSource) -> pd.DataFrame:

        if self.auth:
            session = self.auth.get_session()
        else:
            session = None

        service = ODataService(self.baseroute, reflect_entities=True, session=session)
        entities = service.entities[data_source.entity]
        data = service.query(entities).raw(data_source.query)
        return pd.DataFrame(data)
