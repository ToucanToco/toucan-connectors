from time import time

import pytest

from toucan_connectors.toucan_connector import RetryPolicy


def assert_elapsed(start, stop, expected, error_margin=None):
    if error_margin is None:
        error_margin = min(0.1 * expected, 0.1)
    assert (expected - error_margin) <= stop - start <= (expected + error_margin)


def test_defaut_retry_policy_is_noop():
    retry_policy = RetryPolicy()
    assert retry_policy.retry_decorator() is None


def test_retry_on_single_exception():
    retry_policy = RetryPolicy(retry_on=(KeyError,))
    logbook = []

    @retry_policy.retry_decorator()
    def myfunc():
        if not logbook:
            logbook.append(None)
            raise KeyError()

    myfunc()
    assert len(logbook) == 1


def test_no_retry_on_unexpectd_exception():
    retry_policy = RetryPolicy(retry_on=(KeyError,))
    logbook = []

    @retry_policy.retry_decorator()
    def myfunc():
        if not logbook:
            logbook.append(None)
            raise ValueError()

    with pytest.raises(ValueError):
        myfunc()
    assert len(logbook) == 1


def test_retry_on_multiple_exceptions():
    retry_policy = RetryPolicy(retry_on=(KeyError, ValueError))
    logbook = []

    @retry_policy.retry_decorator()
    def myfunc():
        if not logbook:
            logbook.append(None)
            raise KeyError()
        if len(logbook) == 1:
            logbook.append(None)
            raise ValueError()

    myfunc()
    assert len(logbook) == 2


def test_max_attempts():
    retry_policy = RetryPolicy(max_attempts=3)
    logbook = []

    @retry_policy.retry_decorator()
    def myfunc(max_attempts):
        if len(logbook) < max_attempts:
            logbook.append(None)
            raise RuntimeError("try again!")

    myfunc(1)
    assert len(logbook) == 1
    logbook.clear()
    myfunc(2)
    assert len(logbook) == 2
    logbook.clear()
    with pytest.raises(RuntimeError):
        myfunc(3)
    assert len(logbook) == 3


def test_max_delay():
    retry_policy = RetryPolicy(max_delay=1)
    logbook = [None]

    @retry_policy.retry_decorator()
    def myfunc():
        logbook[0] = time()
        raise RuntimeError("try again!")

    with pytest.raises(RuntimeError):
        approx_start = time()
        myfunc()
    end = logbook[0]
    assert_elapsed(approx_start, end, 1)


def test_wait_time():
    retry_policy = RetryPolicy(wait_time=0.5)
    logbook = []

    @retry_policy.retry_decorator()
    def myfunc():
        if len(logbook) < 3:
            logbook.append(time())
            raise RuntimeError("try again!")

    myfunc()
    assert len(logbook) == 3
    t1, t2, t3 = logbook
    assert_elapsed(t1, t2, 0.5)
    assert_elapsed(t2, t3, 0.5)


def test_mix_attempts_and_max_delay():
    retry_policy = RetryPolicy(wait_time=0.5, max_attempts=10, max_delay=2)
    logbook = []

    @retry_policy.retry_decorator()
    def myfunc():
        logbook.append(time())
        raise RuntimeError("try again!")

    with pytest.raises(RuntimeError):
        approx_start = time()
        myfunc()
    assert len(logbook) == 5
    last_time = logbook[-1]
    assert_elapsed(approx_start, last_time, 2)
