import asyncio
import json
from datetime import datetime, timedelta
from itertools import chain
from typing import List, Optional

import pandas as pd
import requests
from aiohttp import ClientSession

from toucan_connectors.common import get_loop
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

_TOKEN_CACHE = None  # internal cache to avoid re-requesting OAUTH access_token


async def fetch(session, url):
    """aiohttp version of requests.get(...).json()"""
    async with session.get(url) as response:
        return json.loads(await response.read())


async def _batch_fetch(urls):
    """fetch asyncrhonously `urls` in a single batch"""
    async with ClientSession() as session:
        tasks = (asyncio.Task(fetch(session, url)) for url in urls)
        return await asyncio.gather(*tasks)


def batch_fetch(urls):
    """fetch asyncrhonously `urls` in a single batch"""
    loop = get_loop()
    future = asyncio.ensure_future(_batch_fetch(urls))
    return loop.run_until_complete(future)


def fetch_wootric_data(query, props_fetched=None, batch_size=5, max_pages=30):
    """call the `query` wootric API endpoint and handle pagination

    Parameters:

    - `query`: the API endpoint, e.g. `'response'`

    - `props_fetched`: if specified, a list of properties to pick in the json documents
      returned by wootric

    - `batch_size`: number of requests to batch together. i.e. if `batch_size=5`, then the
      API will be queried by batches of 5 queries until data is exhausted or `max_pages`
      is reached.

    - `max_pages`: maximum number of pages to crawl.
    """
    all_data = []
    for page in range(1, max_pages + 1, batch_size):
        lastpage = min(page + batch_size, max_pages + 1)
        urls = [f'{query}&page={pagenum}' for pagenum in range(page, lastpage)]
        responses = batch_fetch(urls)
        data = chain.from_iterable(responses)
        if props_fetched is None:
            all_data.extend(data)
        else:
            all_data.extend([{prop: d[prop] for prop in props_fetched} for d in data])
        # last response is empty, it means that wootric doesn't have any data left
        if not responses[-1]:
            break
    return all_data


def access_token(connector):
    """return OAUTH access token for connector `connector`

    This function handles a cache internally to avoid re-requesting the token
    if the one is cached is still valid.
    """
    global _TOKEN_CACHE
    if _TOKEN_CACHE is not None:
        token_infos = _TOKEN_CACHE
    else:
        token_infos = {}
    now = datetime.now()
    if not token_infos or token_infos.get('expiration-date') < now:
        token_infos = connector.fetch_access_token()
        _TOKEN_CACHE = token_infos
    return token_infos['access_token']


def wootric_url(route):
    """helper to build a full wootric API route, handling leading '/'

    >>> wootric_url('v1/responses')
    ''https://api.wootric.com/v1/responses'
    >>> wootric_url('/v1/responses')
    ''https://api.wootric.com/v1/responses'
    """
    route = route.lstrip('/')
    return f'https://api.wootric.com/{route}'


class WootricDataSource(ToucanDataSource):
    query: str
    properties: Optional[List[str]] = None
    batch_size: int = 5
    max_pages: int = 30


class WootricConnector(ToucanConnector):
    data_source_model: WootricDataSource

    client_id: str
    client_secret: str
    api_version: str = 'v1'

    def fetch_access_token(self):
        """fetch OAUH access token

        cf. https://docs.wootric.com/api/#authentication
        """
        response = requests.post(
            wootric_url('oauth/token'),
            data={
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'client_credentials',
            },
        ).json()
        return {
            'access_token': response['access_token'],
            'expiration-date': datetime.now() + timedelta(seconds=int(response['expires_in'])),
        }

    def _retrieve_data(self, data_source: WootricDataSource) -> pd.DataFrame:
        """Return the concatenated data for all pages."""
        baseroute = wootric_url(f'{self.api_version}/{data_source.query}')
        query = f'{baseroute}?access_token={access_token(self)}'
        all_data = fetch_wootric_data(
            query,
            props_fetched=data_source.properties,
            batch_size=data_source.batch_size,
            max_pages=data_source.max_pages,
        )
        return pd.DataFrame(all_data)
