"""Helpers functions for setting up tests for AirCall connector"""

from tests.general_helpers import check_py_version
from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource

# we want this module's code to be checked against Python3.8
# versions prior to Python3.8 don't handle mocks same way for async functions
PY_VERSION_TO_CHECK = (3, 8)

is_py_version_older = check_py_version(PY_VERSION_TO_CHECK)


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
    if is_py_version_older:
        if expected_count is not None:
            assert fake_func.call_count == expected_count
        if expected_params:
            fake_func.assert_called_with(*expected_params)
    else:
        if expected_count is not None:
            assert fake_func.await_count == expected_count
        if expected_params:
            fake_func.assert_awaited_with(*expected_params)
        else:
            fake_func.assert_awaited_once()
