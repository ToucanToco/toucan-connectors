import logging
import threading
import time
import types
from typing import Optional, Dict
from enum import Enum

logger = logging.getLogger(__name__)


class Status(Enum):
    READY = 'ready'
    IN_PROGRESS = 'in progress'
    CLOSE_IN_PROGRESS = 'is_closing'


class ConnectionObject:
    status: Status
    t_start: float
    t_ready: float
    t_get: float
    alive: Optional[types.FunctionType] = None
    close: Optional[types.FunctionType] = None
    connection: Optional = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.t_start = time.time()

    def is_ready(self):
        return self.status == Status.READY

    def update(self,  **kwargs):
        self.__dict__.update(kwargs)
        self.t_ready = time.time()
        self.t_get = time.time()

    def get_connection(self):
        self.t_get = time.time()
        return self.connection

    def exec_alive(self):
        if self.alive and isinstance(self.alive, types.FunctionType):
            result = self.alive()
            logger.debug(f'Connection alive result {result}')
            return result
        else:
            logger.warning(
                'Alive connection needed but no alive method defined or alive method is not callable'
            )
            return True

    def exec_close(self):
        self.status = Status.CLOSE_IN_PROGRESS
        if self.close and isinstance(self.close, types.FunctionType):
            return self.close()
        else:
            logger.warning(
                'Close connection needed but no close method defined or close method is not callable'
            )


class ConnectionManager:
    name = 'connection_manager'

    cm = {}

    timeout = 5
    wait = 0.2
    time_between_clean = 10
    time_keep_alive = 600
    connection_timeout = 60

    clean_active: bool = False

    lock: bool = False

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.cm: Dict[str, ConnectionObject] = {}
        self.__activate_clean()

    def __close(self, key):
        self.cm[key].exec_close()
        del self.cm[key]

    def __activate_clean(self, active: Optional[bool] = False):
        if len(self.cm) > 0 and (not self.clean_active or active):
            self.clean_active = True
            threading.Timer(self.time_between_clean, self._clean).start()
        else:
            self.clean_active = False

    def __remove(self, cm_to_remove):
        for key in cm_to_remove:
            self.__close(key)

    def _clean(self):
        logger.debug(f'Check if connection is alive ({len(self.cm)} open connections)')

        self.lock = True

        cm_to_remove = []
        identifier: str
        cm: ConnectionObject
        for identifier, cm in list(self.cm.items()):
            tt = time.time()
            if cm.is_ready():
                if not cm.exec_alive():
                    logger.debug(f'Close connection - connection not alive')
                    cm_to_remove.append(identifier)
                elif tt - cm.t_get > self.time_keep_alive:
                    logger.debug(f'Close connection - alive too long ({tt - cm.t_get} > {self.time_keep_alive})')
                    cm_to_remove.append(identifier)
            elif not cm.is_ready() and tt - cm.t_start > self.connection_timeout:
                logger.debug(f'Close connection - connection too long ({tt - cm.t_start} > {self.connection_timeout})')
                cm_to_remove.append(identifier)

        if len(cm_to_remove) > 0:
            self.__remove(cm_to_remove)
        self.lock = False
        self.__activate_clean(True)

    def _create(self, identifier: str, connect_method, alive_method, close_method):
        if isinstance(connect_method, types.FunctionType):
            self.cm[identifier] = ConnectionObject(
                status=Status.IN_PROGRESS,
                alive=alive_method,
                close=close_method,
            )

            self.__activate_clean()
            c = connect_method()
            if identifier in self.cm:
                self.cm[identifier].update(
                    status=Status.READY,
                    connection=c
                )
                return self.cm[identifier].get_connection()
            else:
                return None
        else:
            return None

    def _get_wait(
        self, identifier: str, connect_method, alive_method, close_method, retry_time: float
    ):
        if identifier not in self.cm:
            return self._create(identifier, connect_method, alive_method, close_method)
        elif (
            self.lock
            or not self.cm[identifier].is_ready()
        ):
            logger.debug('Connection is in progress')
            time.sleep(self.wait)
            if self.wait + retry_time < self.timeout:
                return self._get_wait(
                    identifier, connect_method, alive_method, close_method, self.wait + retry_time
                )
            else:
                logger.error(
                    f'Timeout - Impossible to retrieve connection (Timeout: {self.timeout})'
                )
                raise TimeoutError(f'Impossible to retrieve connection (Timeout: {self.timeout})')
        elif self.cm[identifier].is_ready():
            logger.debug('Connection is ready')
            return self.cm[identifier].get_connection()

    def get(self, identifier: str, connect_method, alive_method, close_method):
        logger.debug(f'Get element in Dict {identifier}')
        if identifier in self.cm:
            logging.getLogger(__name__).debug('Connection exist')
            return self._get_wait(identifier, connect_method, alive_method, close_method, 0)
        else:
            logger.debug('Connection does not exist, create and save it')
            return self._create(identifier, connect_method, alive_method, close_method)

    def force_clean(self):
        self.lock = True
        co: ConnectionObject
        identifier: str
        for identifier, co in list(self.cm.items()):
            if co.is_ready():
                co.exec_close()
            del self.cm[identifier]
        self.lock = False
