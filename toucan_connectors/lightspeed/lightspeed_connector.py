import pandas as pd
import pyjq
from pydantic import Field

from toucan_connectors.common import FilterSchema, nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class LightspeedDataSource(ToucanDataSource):
    endpoint: str = Field(
        ...,
        title='Endpoint of the Lightspeed API',
        description='See https://developers.lightspeedhq.com/retail/endpoints/Account/',
    )
    filter: str = FilterSchema


class LightspeedConnector(ToucanConnector):
    """
    This is a connector for [Lightspeed](https://developers.lightspeedhq.com/retail/endpoints/Account/)
    using [Bearer.sh](https://app.bearer.sh/)
    """

    data_source_model: LightspeedDataSource
    bearer_integration = 'lightspeed'
    bearer_auth_id: str

    def _retrieve_data(self, data_source: LightspeedDataSource) -> pd.DataFrame:
        endpoint = nosql_apply_parameters_to_query(data_source.endpoint, data_source.parameters)
        data = self.bearer_oauth_get_endpoint(endpoint)
        data = pyjq.first(data_source.filter, data)
        return pd.DataFrame(data)
