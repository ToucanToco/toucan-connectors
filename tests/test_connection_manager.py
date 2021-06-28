from threading import Thread
from typing import Optional

import pytest
import time
from toucan_connectors.connection_manager import ConnectionManager


@pytest.fixture
def connection_manager():
    return ConnectionManager(
        name='connection_manager_name',
        timeout=3,
        wait=0.2,
        time_between_clean=3,
        time_keep_alive=5
    )


def _get_connection(cm: ConnectionManager, identifier: str, sleep: Optional[int] = 0):
    def __connect():
        if 0 < sleep:
            for i in range(sleep):
                pass
        return ConnectionObject()

    def __alive():
        return True

    def __close():
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


def test_waiting_connection(connection_manager, mocker):
    spy = mocker.spy(connection_manager, '_get_wait')
    p1 = Thread(target=_get_connection, args=(connection_manager, 'conn_1', 100000000))
    p2 = Thread(target=_get_connection, args=(connection_manager, 'conn_1', 100))
    p1.start()
    time.sleep(0.3)
    p2.start()
    p1.join()
    p2.join()

    assert spy.call_count > 3


def test_clean(connection_manager):
    conn: ConnectionObject = _get_connection(connection_manager, 'conn_1')
    assert len(connection_manager.cm) == 1
    time.sleep(5)
    assert len(connection_manager.cm) == 0

