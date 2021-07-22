import contextlib
import logging
import threading
import time
import types
from enum import Enum
from typing import Dict, Optional, Union

logger = logging.getLogger(__name__)


class Status(Enum):
    NOT_START = 'connection not start'
    AVAILABLE = 'connection available'
    QUERY_IN_PROGRESS = 'query in progress'
    CONNECTION_IN_PROGRESS = 'connection in progress'
    CLOSE_IN_PROGRESS = 'connection closing in progress'
    CLOSED = 'connection closed'


class ConnectionBO:
    def __init__(self, **kwargs):
        self.status: Status = Status.NOT_START
        # Connection begin
        self.t_start: float = time.time()

        # Connection is created and available to execute request
        self.t_ready: float = 0

        # Connection used to execute request
        self.t_get: float = time.time()

        # method to check if connection is open or closed
        self.alive: Optional[Union[types.FunctionType, types.MethodType]] = None

        # method to close the connection
        self.close: Optional[Union[types.FunctionType, types.MethodType]] = None

        # method to open the connection
        self.connect: Optional[Union[types.FunctionType, types.MethodType]] = None

        # The connection
        self.connection: Optional = None

        # Number of retry to close the connection
        self.remove_try = 0

        for k, v in kwargs.items():
            if k in self.__dict__:
                setattr(self, k, v)
            else:
                raise KeyError(k)

    def is_ready(self):
        return self.status == Status.AVAILABLE or self.status == Status.QUERY_IN_PROGRESS

    def update(self, status: Status, connection):
        self.status = status
        self.connection = connection
        self.t_ready = time.time()
        self.t_get = time.time()

    @contextlib.contextmanager
    def get_connection(self):
        self.t_get = time.time()
        self.status = Status.QUERY_IN_PROGRESS
        yield self.connection
        self.status = Status.AVAILABLE

    def exec_alive(self):
        try:
            result = self.alive(self.connection)
            logger.debug(f'Connection alive result {result}')
            return result
        except Exception as e:
            logger.warning(
                'Alive connection needed but no alive method defined or alive method is not callable'
            )
            raise e

    def exec_close(self) -> bool:
        self.status = Status.CLOSE_IN_PROGRESS
        if self.force_to_remove():
            self.status = Status.CLOSED
            return True
        else:
            try:
                self.close(self.connection)
                self.status = Status.CLOSED
                return True
            except Exception as e:
                self.status = Status.AVAILABLE
                self.remove_try += 1
                raise e

    def force_to_remove(self):
        """Return True when closing or if check alive raises Exception 3 times."""
        return self.remove_try >= 3


class ConnectionManager:
    def __init__(self, **kwargs):
        self.name: str = 'connection_manager'
        self.connection_list: Dict[str, ConnectionBO] = {}

        self.timeout = 5
        self.wait = 0.2
        self.time_between_clean = 10
        self.time_keep_alive = 600
        self.connection_timeout = 60

        self.clean_active: bool = False

        self.lock: bool = False

        for k, v in kwargs.items():
            if k in self.__dict__:
                setattr(self, k, v)
            else:
                raise KeyError(k)

    def _remove(self, list_connection_to_remove):
        for identifier in list_connection_to_remove:
            try:
                is_closed = self.connection_list[identifier].exec_close()
                if is_closed:
                    del self.connection_list[identifier]
            except Exception:
                continue

    def _clean(self):
        logger.debug(
            f'{self} - Check if connection exists ({len(self.connection_list)} open connections)'
        )

        self.lock = True

        list_connection_to_remove = []
        identifier: str
        co: ConnectionBO
        for identifier, co in list(self.connection_list.items()):
            tt = time.time()
            try:
                if co.is_ready():
                    if not co.exec_alive():
                        logger.debug('Close connection - connection not alive')
                        list_connection_to_remove.append(identifier)
                    elif co.t_get and tt - co.t_get > self.time_keep_alive:
                        logger.debug(
                            f'Close connection - alive too long ({tt - co.t_get} > {self.time_keep_alive})'
                        )
                        list_connection_to_remove.append(identifier)
                elif not co.is_ready() and co.t_start and tt - co.t_start > self.connection_timeout:
                    logger.debug(
                        f'Close connection - connection too long ({tt - co.t_start} > {self.connection_timeout})'
                    )
                    list_connection_to_remove.append(identifier)
            except Exception:
                if co.t_get and tt - co.t_get > self.time_keep_alive:
                    logger.debug(
                        f'Close connection - alive too long ({tt - co.t_get} > {self.time_keep_alive})'
                    )
                    list_connection_to_remove.append(identifier)
                continue

        if len(list_connection_to_remove) > 0:
            self._remove(list_connection_to_remove)

        self.lock = False

        self.clean_active = False
        self._activate_clean()

    def _activate_clean(self, active: Optional[bool] = False):
        if len(self.connection_list) > 0 and (not self.clean_active or active):
            self.clean_active = True
            t_clean = threading.Timer(self.time_between_clean, self._clean)
            t_clean.start()
        else:
            self.clean_active = False

    def _create(
        self,
        identifier: Optional[str],
        connect_method,
        alive_method,
        close_method,
        save: bool = True,
    ):
        if isinstance(connect_method, types.FunctionType) or isinstance(
            connect_method, types.MethodType
        ):
            cbo = ConnectionBO(
                status=Status.CONNECTION_IN_PROGRESS,
                connect=connect_method,
                alive=alive_method,
                close=close_method,
            )
            if save:
                self.connection_list[identifier] = cbo

            self._activate_clean()
            c = connect_method()
            if save:
                self.connection_list[identifier].update(status=Status.AVAILABLE, connection=c)
            else:
                cbo.update(status=Status.AVAILABLE, connection=c)
            return cbo.get_connection()
        else:
            raise Exception('Connection is not a method')

    def get(self, identifier: str, connect_method, alive_method, close_method, save: bool = True):
        """Retrieve or create connection if not exist in connection_list"""
        logger.debug(f'Get element in Dict {identifier}')
        if (
            identifier is not None
            and identifier in self.connection_list
            and self.connection_list[identifier].is_ready()
        ):
            logging.getLogger(__name__).debug('Connection exist')
            return self.connection_list[identifier].get_connection()
        else:
            logger.debug('Connection does not exist, create and save it')
            return self._create(identifier, connect_method, alive_method, close_method, save)

    def force_clean(self):
        """
        Force to remove all connection
        Not use automatically by connection_manager
        """
        self.lock = True
        co: ConnectionBO
        identifier: str
        for identifier, co in list(self.connection_list.items()):
            if co.is_ready():
                co.exec_close()
            del self.connection_list[identifier]
        self.lock = False
