import concurrent.futures
import time
from typing import Optional

import pytest
from mock import patch

from toucan_connectors.connection_manager import ConnectionBO, ConnectionManager


@pytest.fixture
def connection_manager():
    return ConnectionManager(
        name='connection_manager_name',
        timeout=3,
        wait=0.2,
        time_between_clean=1,
        time_keep_alive=5,
        connection_timeout=60,
    )


@pytest.fixture
def connection_manager_with_error():
    return ConnectionManager(
        name='connection_manager_name',
        timeout=3,
        wait=0.2,
        time_between_clean=1,
        time_keep_alive=5,
        connection_timeout=60,
        tortank='test',
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

    def __alive(conn):
        if enabled_alive:
            return True
        else:
            return False

    def __close(conn):
        return True

    connection = cm.get(
        identifier,
        connect_method=__connect,
        alive_method=__alive,
        close_method=__close,
        save=True,
    )
    return connection


def _get_connection_exception(cm: ConnectionManager, identifier: str):
    def __connect():
        return ConnectionObject()

    def __alive(conn):
        return True

    def __close(conn):
        raise TimeoutError

    connection = cm.get(
        identifier,
        connect_method=__connect,
        alive_method=__alive,
        close_method=__close,
    )
    return connection


def _get_connection_without_connect_method(
    cm: ConnectionManager,
    identifier: str,
):
    def __alive(conn):
        return True

    def __close(conn):
        return True

    connection = cm.get(
        identifier,
        connect_method='connect',
        alive_method=__alive,
        close_method=__close,
    )
    return connection


def _get_connection_without_alive_close_method(
    cm: ConnectionManager,
    identifier: str,
):
    def __connect():
        return ConnectionObject()

    connection = cm.get(
        identifier,
        connect_method=__connect,
        alive_method='alive',
        close_method='close',
    )
    return connection


def _get_connection_long_closing(cm: ConnectionManager, identifier: str, time_sleep: int):
    def __connect():
        return ConnectionObject()

    def __alive(conn):
        return True

    def __close(conn):
        time.sleep(time_sleep)
        return True

    connection = cm.get(
        identifier,
        connect_method=__connect,
        alive_method=__alive,
        close_method=__close,
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


def test_init_exception_connectionbo():
    with pytest.raises(KeyError):
        ConnectionBO(tortank='test')


def test_init_exception_connectionmanager():
    with pytest.raises(KeyError):
        ConnectionManager(
            name='connection_manager_name',
            timeout=3,
            wait=0.2,
            time_between_clean=1,
            time_keep_alive=5,
            connection_timeout=60,
            tortank='test',
        )


def test_get_basic(connection_manager):
    with _get_connection(connection_manager, 'conn_1') as conn:
        assert len(connection_manager.connection_list) == 1
        assert conn.name == 'ConnectionObject'
        connection_manager.force_clean()


def test_multiple_same_get(connection_manager):
    with _get_connection(connection_manager, 'conn_1') as conn, _get_connection(
        connection_manager, 'conn_1'
    ) as conn2:
        assert len(connection_manager.connection_list) == 1
        assert conn == conn2
        connection_manager.force_clean()


def test_multiple_different_get(connection_manager):
    with _get_connection(connection_manager, 'conn_1') as conn, _get_connection(
        connection_manager, 'conn_2'
    ) as conn2:
        assert len(connection_manager.connection_list) == 2
        assert conn != conn2
        connection_manager.force_clean()


def test_waiting_connection_success(connection_manager, mocker):
    spy = mocker.spy(connection_manager, '_ConnectionManager__get_wait')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_get_connection, connection_manager, 'conn_1', 0.1),
            executor.submit(_get_connection, connection_manager, 'conn_1', 0.6),
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.exception() is not None:
                raise future.exception()
        assert spy.call_count >= 1
    connection_manager.force_clean()


def test_waiting_connection_timeout(connection_manager, mocker):
    spy = mocker.spy(connection_manager, '_ConnectionManager__get_wait')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(TimeoutError):
            futures = [executor.submit(_get_connection, connection_manager, 'conn_1', 4)]
            time.sleep(0.3)
            futures.append(executor.submit(_get_connection, connection_manager, 'conn_1', 0))
            for future in concurrent.futures.as_completed(futures):
                if future.exception() is not None:
                    raise future.exception()
            assert spy.call_count >= 1
    connection_manager.force_clean()


def test_auto_clean_exception(connection_manager):
    t1 = connection_manager.time_keep_alive
    t2 = connection_manager.time_between_clean
    connection_manager.time_keep_alive = 1
    connection_manager.time_between_clean = 1

    with _get_connection_exception(connection_manager, 'conn_1'):
        assert len(connection_manager.connection_list) == 1
    time.sleep(6)
    assert len(connection_manager.connection_list) == 0
    connection_manager.time_keep_alive = t1
    connection_manager.time_between_clean = t2
    connection_manager.force_clean()


@patch('toucan_connectors.connection_manager.ConnectionBO.force_to_remove', return_value=True)
def test_auto_clean_force_remove(rs, connection_manager):
    t1 = connection_manager.time_keep_alive
    t2 = connection_manager.time_between_clean
    connection_manager.time_keep_alive = 1
    connection_manager.time_between_clean = 1

    _get_connection(connection_manager, 'conn_1')
    assert len(connection_manager.connection_list) == 1
    time.sleep(3)
    assert len(connection_manager.connection_list) == 0
    connection_manager.time_keep_alive = t1
    connection_manager.time_between_clean = t2
    connection_manager.force_clean()


def test_auto_clean_simple(connection_manager):
    t1 = connection_manager.time_keep_alive
    connection_manager.time_keep_alive = 1

    with _get_connection(connection_manager, 'conn_1'):
        assert len(connection_manager.connection_list) == 1
    time.sleep(3)
    assert len(connection_manager.connection_list) == 0
    connection_manager.time_keep_alive = t1
    connection_manager.force_clean()


def test_auto_clean_multiple(connection_manager):
    t1 = connection_manager.time_keep_alive
    t2 = connection_manager.time_between_clean
    connection_manager.time_keep_alive = 2
    connection_manager.time_between_clean = 1
    with _get_connection(connection_manager, 'conn_1'), _get_connection(
        connection_manager, 'conn_2'
    ):
        assert len(connection_manager.connection_list) == 2
        time.sleep(1)
        with _get_connection(connection_manager, 'conn_3'):
            assert len(connection_manager.connection_list) == 3
    time.sleep(1)
    assert len(connection_manager.connection_list) == 1
    connection_manager.time_keep_alive = t1
    connection_manager.time_between_clean = t2
    connection_manager.force_clean()


def test_force_clean(connection_manager):
    with _get_connection(connection_manager, 'conn_1'), _get_connection(
        connection_manager, 'conn_2'
    ):
        assert len(connection_manager.connection_list) == 2
    connection_manager.force_clean()
    assert len(connection_manager.connection_list) == 0
    connection_manager.force_clean()


def test_clean_connection_not_alive(connection_manager):
    with _get_connection(connection_manager, 'conn_1', enabled_alive=False):
        assert len(connection_manager.connection_list) == 1
    time.sleep(1)
    assert len(connection_manager.connection_list) == 0
    connection_manager.force_clean()


def test_remove_connection_in_progress_too_long(connection_manager):
    t1 = connection_manager.connection_timeout
    connection_manager.connection_timeout = 1

    with pytest.raises(Exception):
        with _get_connection(connection_manager, 'conn_1', sleep=3):
            assert len(connection_manager.connection_list) == 0

    connection_manager.connection_timeout = t1
    connection_manager.force_clean()


def test_connection_manager_without_close_method_define(connection_manager):
    t1 = connection_manager.time_keep_alive
    t2 = connection_manager.time_between_clean
    connection_manager.time_keep_alive = 1
    with _get_connection_without_alive_close_method(connection_manager, 'conn_1'):
        assert len(connection_manager.connection_list) == 1
    time.sleep(5)
    assert len(connection_manager.connection_list) == 0
    connection_manager.time_keep_alive = t1
    connection_manager.time_between_clean = t2
    connection_manager.force_clean()


def test_connect_method_is_not_callable(connection_manager):
    with pytest.raises(Exception):
        with _get_connection_without_connect_method(connection_manager, 'conn_1'):
            assert len(connection_manager.connection_list) == 0
    connection_manager.force_clean()


def test_get_wait_lock(connection_manager, mocker):
    t1 = connection_manager.time_keep_alive
    t2 = connection_manager.time_between_clean
    connection_manager.time_keep_alive = 1
    connection_manager.time_between_clean = 1
    spy = mocker.spy(connection_manager, '_ConnectionManager__get_wait')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(_get_connection_long_closing, connection_manager, 'conn_1', 2)]
        time.sleep(0.3)
        assert not connection_manager.lock
        time.sleep(1)
        assert connection_manager.lock
        futures.append(executor.submit(_get_connection, connection_manager, 'conn_1'))
        for future in concurrent.futures.as_completed(futures):
            if future.exception() is not None:
                raise future.exception()
        assert spy.call_count > 1
    connection_manager.time_keep_alive = t1
    connection_manager.time_between_clean = t2
    connection_manager.force_clean()
