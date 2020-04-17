"""
Module containing helper functions that can be used in general
throughout tests
"""
import asyncio
import sys

PY_VERSION = sys.version_info


def check_py_version(version_to_check):
    """
    Tests environment's python version vs. a threshold version
    """
    is_older = False
    if PY_VERSION < version_to_check:
        is_older = True
    return is_older


def build_future(fake_data):
    """Builds a single future"""
    f = asyncio.Future()
    f.set_result(fake_data)
    return f
