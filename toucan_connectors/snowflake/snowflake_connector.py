import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class SnowflakeDataSource(ToucanDataSource):
    query: constr(min_lenght=1)

class SnowflakeConnector(ToucanConnector):
    pass

