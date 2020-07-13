from typing import Optional

from toucan_connectors.documentdb.documentdb_translator import DocumentDBConditionTranslator
from toucan_connectors.mongo.mongo_connector import MongoConnector, MongoDataSource

from toucan_connectors.toucan_connector import (
    DataSlice,
    decorate_func_with_retry,
)

MAX_COUNTED_ROWS = 1000001

class DocumentDBConnector(MongoConnector):
    """ Retrieve data from a [DocumentDB](https://aws.amazon.com/documentdb/) database."""
    
    def __init__(self):
        super().__init__()
    
    @decorate_func_with_retry
    def get_slice(
        self,
        data_source: MongoDataSource,
        permissions: Optional[str] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> DataSlice:
        total_count = MAX_COUNTED_ROWS
        data_source = MongoDataSource.parse_obj(data_source)
        
        if offset:
            data_source.query.append({'$skip': offset})
        if limit is not None:
            data_source.query.append({'$limit': limit})
        
        df = self.get_df(data_source, permissions)
        return DataSlice(df, total_count)
