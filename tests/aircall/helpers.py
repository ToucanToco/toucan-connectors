"""Helpers functions for setting up tests for AirCall connector"""
import asyncio
import sys

from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource


def build_con_and_ds(dataset: str):
    """
    Builds test connector and test datasource
    """
    con = AircallConnector(name='mah_test', bearer_auth_id='abc123efg')
    ds = AircallDataSource(name='mah_ds', domain='test_domain', dataset=dataset, limit=1,)

    return con, ds


def handle_mock_data(fake_data):
    # In python > 3.8, patch detects we're mocking a coroutine and replace it by an AsyncMock
    if sys.version_info > (3, 8):
        return fake_data
    # In python < 3.8, patch only uses a MagicMock, which is not awaitable
    else:
        if len(fake_data) > 1:
            return build_futures(fake_data)
        else:
            return build_future(fake_data)


def build_future(fake_data):
    f = asyncio.Future()
    f.set_result(fake_data)
    return f


def build_futures(fake_data):
    fake_teams, fake_users = fake_data
    f_teams = asyncio.Future()
    f_teams.set_result(fake_teams)
    f_users = asyncio.Future()
    f_users.set_result(fake_users)
    return [f_teams, f_users]
