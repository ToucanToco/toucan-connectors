"""Helpers functions for setting up tests for AirCall connector"""

from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource


def build_con_and_ds(dataset: str):
    """
    Builds test connector and test datasource for testing with API key

    Leave this function in if ever want to run tests without skipping
    due to there being no Bearer tokens
    How to use:
    Replace build_ds function with this one in test_aircall file
    Be sure to also replace the endpoints inside the aircall connector file
    """
    con = AircallConnector(name='mah_test', bearer_auth_id='abc123efg')
    ds = AircallDataSource(
        name='mah_ds',
        domain='test_domain',
        dataset=dataset,
        limit=1,
    )

    return con, ds


def assert_called_with(fake_func, expected_params=[], expected_count=None):
    """Tests mock function with called/awaited depending on Python version"""
    if expected_count is not None:
        assert fake_func.await_count == expected_count
    if expected_params:
        fake_func.assert_awaited_with(*expected_params)
    else:
        fake_func.assert_awaited_once()
