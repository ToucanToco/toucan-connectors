import logging
import threading
import time
import types
from typing import Optional

logger = logging.getLogger(__name__)


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
        self.cm = {}
        self.__activate_clean()

    def __close(self, key):
        self.cm[key]['status'] = 'is_closing'
        if self.cm[key]['close'] and isinstance(self.cm[key]['close'], types.FunctionType):
            logger.debug('Close connexion call')
            self.cm[key]['close']()
        else:
            logger.warning(
                'Close connexion needed but no close method defined or close method is not callable'
            )
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
        logger.debug(f'Check if connexion is alive ({len(self.cm)} open connexions)')

        self.lock = True

        cm_to_remove = []
        for identifier, cm in list(self.cm.items()):
            tt = time.time()
            if 'ready' == cm['status']:
                if tt - cm['t_get'] > self.time_keep_alive:
                    logger.debug(f'Close connexion {tt - cm["t_get"]} > {self.time_keep_alive}')
                    cm_to_remove.append(identifier)
                elif (
                    cm['alive']
                    and isinstance(cm['alive'], types.FunctionType)
                    and not cm['alive']()
                ):
                    cm_to_remove.append(identifier)
            elif 'in_progress' == cm['status'] and tt - cm['t_start'] > self.connection_timeout:
                cm_to_remove.append(identifier)

        if len(cm_to_remove) > 0:
            self.__remove(cm_to_remove)
        self.lock = False
        self.__activate_clean(True)

    def _create(self, identifier: str, connect_method, alive_method, close_method):
        if isinstance(connect_method, types.FunctionType):
            self.cm[identifier] = {
                'status': 'in_progress',
                't_start': time.time(),
            }

            self.__activate_clean()
            c = connect_method()
            self.cm[identifier] = {
                'status': 'ready',
                't_ready': time.time(),
                't_get': time.time(),
                'alive': alive_method,
                'close': close_method,
                'connection': c,
            }
            return self.cm[identifier]['connection']
        else:
            return None

    def _get_wait(
        self, identifier: str, connect_method, alive_method, close_method, retry_time: float
    ):
        if identifier not in self.cm:
            return self._create(identifier, connect_method, alive_method, close_method)
        elif (
            self.lock
            or 'in_progress' == self.cm[identifier]['status']
            or 'is_closing' == self.cm[identifier]['status']
        ):
            logger.debug('Connection is in progress')
            time.sleep(self.wait)
            if self.wait + retry_time < self.timeout:
                return self._get_wait(
                    identifier, connect_method, alive_method, close_method, self.wait + retry_time
                )
            else:
                logger.error(
                    f'Timeout - Impossible to retrieve connexion (Timeout: {self.timeout})'
                )
                raise TimeoutError(f'Impossible to retrieve connexion (Timeout: {self.timeout})')
        elif 'ready' == self.cm[identifier]['status']:
            logger.debug('Connection is ready')
            self.cm[identifier]['t_get'] = time.time()
            return self.cm[identifier]['connection']

    def get(self, identifier: str, connect_method, alive_method, close_method):
        logger.debug(f'Get element in Dict {identifier}')
        if identifier in self.cm:
            logging.getLogger(__name__).debug('Connection exist')
            return self._get_wait(identifier, connect_method, alive_method, close_method, 0)
        else:
            logger.debug('Connection not exist, create and save it')
            return self._create(identifier, connect_method, alive_method, close_method)

    def force_clean(self):
        self.lock = True
        for key, value in list(self.cm.items()):
            if 'ready' == self.cm[key]['status']:
                self.__close(key)
            else:
                del self.cm[key]
        self.lock = False
