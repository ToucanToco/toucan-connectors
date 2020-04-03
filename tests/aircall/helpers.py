"""Helpers functions for setting up tests for AirCall connector"""
import asyncio

from tests.aircall.mock_results import fake_teams, fake_users
from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource


def build_con_and_ds(dataset: str):
    """
    Builds test connector and test datasource
    """
    con = AircallConnector(name='mah_test', bearer_auth_id='abc123efg')
    ds = AircallDataSource(name='mah_ds', domain='test_domain', dataset=dataset, limit=1,)

    return con, ds


def build_mock_fetch_data(fake_data, mocker):
    """
    Builds a mock version of the fetch_data function
    """
    f = asyncio.Future()
    f.set_result(fake_data)

    return mocker.patch('toucan_connectors.aircall.aircall_connector.fetch_page', return_value=f)


def build_complex_mock_fetch_data(mocker):
    """
    Builds a mock version of the fetch_data function
    """

    f_1 = asyncio.Future()
    f_1.set_result(fake_teams)
    f_2 = asyncio.Future()
    f_2.set_result(fake_users)

    return mocker.patch(
        'toucan_connectors.aircall.aircall_connector.fetch_page', side_effect=[f_1, f_2]
    )


def run_loop(con, ds):
    """Sets up a real event loop for E2E tests"""
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(con._get_data(ds.dataset, ds.query, ds.limit))
    return loop.run_until_complete(future)
