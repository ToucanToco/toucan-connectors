"""
Revinate connector

Documentation can be found at: https://porter.revinate.com/documentation
"""
import asyncio
import logging
import pandas as pd
from aiohttp import ClientSession
from pydantic import BaseModel, Field, SecretStr
from typing import Optional

from toucan_connectors.common import FilterSchema, get_loop, nosql_apply_parameters_to_query, transform_with_jq
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


LOGGER = logging.getLogger(__file__)


async def fetch(url, session: ClientSession):
    """Generic fetch that returns either an error or a dict"""
    async with session.get(url) as res:
        if res.status != 200:
            LOGGER.error('Revinate API returned following error: %s %s', res.status, res.reason)
            raise Exception(f'Aborting Revinate request due to error from their API: {res.status} {res.reason}')
        return await res.json()


class RevinateAuthentication(BaseModel):
    """
    This class is necessary for calculating the required headers, as such no field is optional
    """
    api_key: str = Field(
        ...,
        title='API Key',
        description='Your API key as provided by Revinate'
    )
    api_secret: SecretStr = Field(
        ...,
        title='API Secret',
        description='Your API secret as provided by Revinate'
    )
    username: str = Field(..., description='Your Revinate username')


class RevinateDataSource(ToucanDataSource):
    """
    The datasource for Revinate

    Must have a valid Revinate endpoint field otherwise the connector won't know which endpoint to call
    The params field is optional but has to be a valid Revinate query field

    cf. https://porter.revinate.com/documentation
    """
    endpoint: str = Field(
        ...,
        description='A valid Revinate endpoint (eg. hotelsets), cf. Resources on https://porter.revinate.com/documentation'
    )
    params: dict = Field(
        None,
        description='JSON object of valid Revinate parameters to send in the query string of this HTTP request (eg. {"page": 2, "size": 20}, which generates a query like https://porter.revinate.com/hotelsets?page=2&size=20), cf. https://porter.revinate.com/documentation'
    )
    filter: str = FilterSchema


class RevinateConnector(ToucanConnector):
    """
    The main Revinate connector

    - It's async
    - It returns a basic pandas Dataframe based on whatever JQ filter is passed to it or it returns an error
    """
    data_source_model: RevinateDataSource
    authentication: RevinateAuthentication

    baseroute = 'https://porter.revinate.com'

    async def _get_data(self, query, jq_filter):
        """
        Basic data retrieval function

        - builds the full url
        - retrieves built headers
        - calls a fetch and returns filtered data or an error
        """

        full_url = f'{self.baseroute}/{query}'
        # build the headers here
        headers = {}

        async with ClientSession(headers=headers) as session:
            data = await fetch(full_url, session)

            try:
                return transform_with_jq(data, jq_filter)
            except ValueError:
                # This follows the HTTP connector's behavior for a similar situation
                LOGGER.error('Could not transform the data using %s as filter', jq_filter)
                raise

    def _run_fetch(self, query, jq_filter):
        """Event loop handler"""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_data(query, jq_filter))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: RevinateDataSource) -> pd.DataFrame:
        """
        Primary function and point of entry
        """
        query = data_source.endpoint
        params_dict: Optional[dict] = None

        if data_source.params:
            params_dict = {k: v for k, v in data_source.params.items()}

        query = nosql_apply_parameters_to_query(query=query, parameters=params_dict)

        result = self._run_fetch(query, jq_filter=data_source.filter)
        return pd.DataFrame(result)
