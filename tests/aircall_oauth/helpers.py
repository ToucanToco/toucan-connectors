"""Helpers functions for setting up tests for AirCall connector"""

def assert_called_with(fake_func, expected_params=[], expected_count=None):
    """Tests mock function with called/awaited depending on Python version"""
    if expected_count is not None:
        assert fake_func.await_count == expected_count
    if expected_params:
        fake_func.assert_awaited_with(*expected_params)
    else:
        fake_func.assert_awaited_once()
