import aiohttp
import asyncio

from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource


def build_ds(dataset):
    con = AircallConnector(name='mah_test', bearer_auth_id='abc123efg')
    ds = AircallDataSource(
        name='mah_ds',
        domain='test_domain',
        dataset=dataset,
        limit=10,
    )

    return con, ds


def run_loop(con, ds):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(con._get_data(ds.dataset, ds.query, ds.limit))
    return loop.run_until_complete(future)
