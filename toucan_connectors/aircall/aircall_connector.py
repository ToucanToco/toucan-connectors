import pandas as pd
from jq import jq
from pydantic import Schema

from toucan_connectors.common import FilterSchema, nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class AircallDataSource(ToucanDataSource):
    endpoint: str = Schema(
        ...,
        title='Endpoint of the Aircall API',
        description='See https://developer.aircall.io/api-references/#endpoints',
    )
    filter: str = FilterSchema


class AircallConnector(ToucanConnector):
    """
    This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    using [Bearer.sh](https://app.bearer.sh/)
    """

    data_source_model: AircallDataSource
    bearer_integration = 'aircall_oauth'
    bearer_auth_id: str

    def _retrieve_data(self, data_source: AircallDataSource) -> pd.DataFrame:
        endpoint = nosql_apply_parameters_to_query(data_source.endpoint, data_source.parameters)
        data = self.bearer_oauth_get_endpoint(endpoint)
        data = jq(data_source.filter).transform(data)
        if isinstance(data, dict):
            data = [data]
        return pd.DataFrame(data)
