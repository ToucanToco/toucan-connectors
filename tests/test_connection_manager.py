import concurrent.futures
import time
from typing import Optional

import pytest

from toucan_connectors.connection_manager import ConnectionManager


@pytest.fixture
def connection_manager():
    return ConnectionManager(
        name='connection_manager_name', timeout=3, wait=0.2, time_between_clean=1, time_keep_alive=5
    )


def _get_connection(
    cm: ConnectionManager,
    identifier: str,
    sleep: Optional[int] = 0,
    enabled_alive: Optional[bool] = True,
):
    def __connect():
        if 0 < sleep:
            time.sleep(sleep)
        return ConnectionObject()

    def __alive():
        if enabled_alive:
            return True
        else:
            return False

    def __close():
        return True

    connection = cm.get(
        identifier,
        connect_method=lambda: __connect(),
        alive_method=lambda: __alive(),
        close_method=lambda: __close(),
    )
    return connection


def _get_connection_without_method(
    cm: ConnectionManager,
    identifier: str,
):
    def __connect():
        return ConnectionObject()

    connection = cm.get(
        identifier,
        connect_method=lambda: __connect(),
        alive_method='alive',
        close_method='close',
    )
    return connection


def _get_connection_long_closing(
    cm: ConnectionManager,
    identifier: str,
):
    def __connect():
        return ConnectionObject()

    def __alive():
        return True

    def __close():
        time.sleep(5)
        return True

    connection = cm.get(
        identifier,
        connect_method=lambda: __connect(),
        alive_method=lambda: __alive(),
        close_method=lambda: __close(),
    )
    return connection


class ConnectionObject:
    name: str = 'ConnectionObject'

    @staticmethod
    def is_closed():
        return True

    @staticmethod
    def close():
        return True


def test_get(connection_manager):
    conn: ConnectionObject = _get_connection(connection_manager, 'conn_1')
    assert len(connection_manager.cm) == 1
    assert conn.name == 'ConnectionObject'


def test_multiple_same_get(connection_manager):
    conn: ConnectionObject = _get_connection(connection_manager, 'conn_1')
    conn2: ConnectionObject = _get_connection(connection_manager, 'conn_1')
    assert len(connection_manager.cm) == 1
    assert conn == conn2


def test_multiple_different_get(connection_manager):
    conn: ConnectionObject = _get_connection(connection_manager, 'conn_1')
    conn2: ConnectionObject = _get_connection(connection_manager, 'conn_2')
    assert len(connection_manager.cm) == 2
    assert conn != conn2


def test_waiting_connection_success(connection_manager, mocker):
    spy = mocker.spy(connection_manager, '_get_wait')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        futures.append(executor.submit(_get_connection, connection_manager, 'conn_1', 0.1))
        futures.append(executor.submit(_get_connection, connection_manager, 'conn_1', 0.6))
        for future in concurrent.futures.as_completed(futures):
            if future.exception() is not None:
                raise future.exception()
        assert spy.call_count > 1


def test_waiting_connection_timeout(connection_manager, mocker):
    spy = mocker.spy(connection_manager, '_get_wait')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(TimeoutError):
            futures = [executor.submit(_get_connection, connection_manager, 'conn_1', 4)]
            time.sleep(0.3)
            futures.append(executor.submit(_get_connection, connection_manager, 'conn_1', 0))
            for future in concurrent.futures.as_completed(futures):
                if future.exception() is not None:
                    raise future.exception()
            assert spy.call_count > 1


def test_auto_clean_simple(connection_manager):
    _get_connection(connection_manager, 'conn_1')
    assert len(connection_manager.cm) == 1
    time.sleep(6)
    assert len(connection_manager.cm) == 0


def test_auto_clean_multiple(connection_manager):
    _get_connection(connection_manager, 'conn_1')
    assert len(connection_manager.cm) == 1
    _get_connection(connection_manager, 'conn_2')
    assert len(connection_manager.cm) == 2
    time.sleep(3)
    assert len(connection_manager.cm) == 2
    _get_connection(connection_manager, 'conn_3')
    assert len(connection_manager.cm) == 3
    time.sleep(3)
    assert len(connection_manager.cm) == 1
    time.sleep(3)
    assert len(connection_manager.cm) == 0


def test_force_clean(connection_manager):
    _get_connection(connection_manager, 'conn_1')
    _get_connection(connection_manager, 'conn_2')
    assert len(connection_manager.cm) == 2
    connection_manager.force_clean()
    assert len(connection_manager.cm) == 0


def test_clean_connection_not_alive(connection_manager):
    _get_connection(connection_manager, 'conn_1', enabled_alive=False)
    assert len(connection_manager.cm) == 1
    time.sleep(1)
    assert len(connection_manager.cm) == 0


# to cover the warning log message
def test_connection_manager_without_close_method_define(connection_manager):
    _get_connection_without_method(connection_manager, 'conn_1')
    assert len(connection_manager.cm) == 1
    time.sleep(5)
    assert len(connection_manager.cm) == 0


def test_connection_manager_lock(connection_manager):
    _get_connection_long_closing(connection_manager, 'conn_1')
    assert len(connection_manager.cm) == 1
    connection_manager.force_clean()
    _get_connection(connection_manager, 'conn_2')
    assert len(connection_manager.cm) == 1
    time.sleep(3)
    assert len(connection_manager.cm) == 1
    assert 'conn_1' not in connection_manager.cm
    assert 'conn_2' in connection_manager.cm
