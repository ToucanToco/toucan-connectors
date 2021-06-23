"""
Revinate connector

Documentation can be found at: https://porter.revinate.com/documentation
"""
import asyncio
import datetime
import logging

import pandas as pd
from _pyjq import ScriptRuntimeError
from aiohttp import ClientSession
from pydantic import BaseModel, Field, SecretStr

from toucan_connectors.common import (
    FilterSchema,
    get_loop,
    nosql_apply_parameters_to_query,
    transform_with_jq,
)
from toucan_connectors.revinate.helpers import build_headers
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

LOGGER = logging.getLogger(__file__)


async def fetch(url, session: ClientSession):
    """Generic fetch that returns either an error or a dict"""
    async with session.get(url) as res:
        if res.status != 200:
            LOGGER.error('Revinate API returned following error: %s %s', res.status, res.reason)
            raise Exception(
                f'Aborting Revinate request due to error from their API: {res.status} {res.reason}'
            )
        return await res.json()


class RevinateAuthentication(BaseModel):
    """
    This class is necessary for calculating the required headers, as such no field is optional

    Requires:
    - an api_key
    - an api_secret
    - a username
    """

    api_key: str = Field(..., title='API Key', description='Your API key as provided by Revinate')
    api_secret: SecretStr = Field(
        '', title='API Secret', description='Your API secret as provided by Revinate'
    )
    username: str = Field(..., description='Your Revinate username')


class RevinateDataSource(ToucanDataSource):
    """
    The datasource for Revinate

    Must have a valid Revinate endpoint field otherwise the connector won't know which endpoint to call
    The params field is optional but has to be a valid Revinate query field

    cf. https://porter.revinate.com/documentation

    Contains:
    - an endpoint (required)
    - optional params
    - a JQ filter (required)
    """

    endpoint: str = Field(
        ...,
        description='A valid Revinate endpoint (eg. hotelsets), cf. Resources on https://porter.revinate.com/documentation',
    )
    params: dict = Field(
        None,
        description='JSON object of valid Revinate parameters to send in the query string of this HTTP request (eg. {"page": 2, "size": 20}, which generates a query like https://porter.revinate.com/hotelsets?page=2&size=20), cf. https://porter.revinate.com/documentation',
    )
    filter: str = FilterSchema


class RevinateConnector(ToucanConnector):
    """
    A connector the Revinate Porter API

    - It's async
    - It returns a basic pandas Dataframe based on whatever JQ filter is passed to it or it returns an error
    """

    data_source_model: RevinateDataSource
    authentication: RevinateAuthentication

    baseroute = 'https://porter.revinate.com'

    async def _get_data(self, endpoint, jq_filter):
        """
        Basic data retrieval function

        - builds the full url
        - retrieves built headers
        - calls a fetch and returns filtered data or an error
        """
        full_url = f'{self.baseroute}/{endpoint}'
        api_key = self.authentication.api_key
        if not self.authentication.api_secret:
            self.authentication.api_secret = SecretStr('')
        api_secret: str = self.authentication.api_secret.get_secret_value()
        username = self.authentication.username
        timestamp = int(datetime.datetime.now().timestamp())

        headers = build_headers(api_key, api_secret, username, str(timestamp))

        async with ClientSession(headers=headers) as session:
            data = await fetch(full_url, session)

            try:
                return transform_with_jq(data['content'], jq_filter)
            except ValueError:
                # This follows the HTTP connector's behavior for a similar situation
                LOGGER.error('Could not transform the data using %s as filter', jq_filter)
                raise
            except ScriptRuntimeError:
                LOGGER.error('Could not transform the data using %s as filter', jq_filter)
                raise

    def _run_fetch(self, endpoint, jq_filter):
        """Event loop handler"""
        loop = get_loop()
        future = asyncio.ensure_future(self._get_data(endpoint, jq_filter))
        return loop.run_until_complete(future)

    def _retrieve_data(self, data_source: RevinateDataSource) -> pd.DataFrame:
        """
        Primary function and point of entry
        """
        endpoint = data_source.endpoint

        endpoint = nosql_apply_parameters_to_query(query=endpoint, parameters=data_source.params)

        result = self._run_fetch(endpoint, jq_filter=data_source.filter)
        return pd.DataFrame(result)
