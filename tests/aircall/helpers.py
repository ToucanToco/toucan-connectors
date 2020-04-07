"""Helpers functions for setting up tests for AirCall connector"""
import asyncio
import sys

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

    # In python > 3.8, patch detects we're mocking a coroutine and replace it by an AsyncMock
    if sys.version_info > (3, 8):
        return_value = fake_data
    # In python < 3.8, patch only uses a MagicMock, which is not awaitable 
    else:
        f = asyncio.Future()
        f.set_result(fake_data)
        return_value = f

    return mocker.patch('toucan_connectors.aircall.aircall_connector.fetch_page', return_value=return_value)


def build_complex_mock_fetch_data(mocker):
    """
    Builds a mock version of the fetch_data function
    """
    if sys.version_info > (3, 8):
        return_value = [fake_teams, fake_users]
    else:
        f_teams = asyncio.Future()
        f_teams.set_result(fake_teams)
        f_users = asyncio.Future()
        f_users.set_result(fake_users)
        return_value = [f_teams, f_users]

    return mocker.patch(
        'toucan_connectors.aircall.aircall_connector.fetch_page', side_effect=return_value
    )
