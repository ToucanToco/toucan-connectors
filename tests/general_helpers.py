"""
Module containing helper functions that can be used in general
throughout tests
"""
import asyncio
import sys

PY_VERSION = sys.version_info
PY_VERSION_TO_CHECK = (3, 8)


def check_py_version(version_to_check):
    """
    Tests environment's python version vs. a threshold version
    """
    is_older = False
    if PY_VERSION < version_to_check:
        is_older = True
    return is_older


def build_future(fake_data):
    """Builds a future for versions of Python older than 3.8"""
    if PY_VERSION < PY_VERSION_TO_CHECK:
        f = asyncio.Future()
        f.set_result(fake_data)
        return f
    return fake_data
