define(`upcase', `translit(`$*', `a-z', `A-Z')')dnl
define(`downcase', `translit(`$*', `A-Z', `a-z')')dnl
define(`cap', `regexp(`$1', `^\(\w\)\(\w*\)', `upcase(`\1')`'downcase(`\2')')')dnl
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class cap(name)DataSource(ToucanDataSource):
    query: str
    
    
class cap(name)Connector(ToucanConnector):
    type = "name"
    data_source_model: cap(name)DataSource
    
    username: str
    password: str
    
    def get_df(self, data_source: cap(name)DataSource) -> pd.DataFrame:
        pass
