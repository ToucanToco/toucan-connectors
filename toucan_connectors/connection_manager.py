import logging
import threading
import time

logger = logging.getLogger(__name__)


class ConnectionManager:
    name = 'connection_manager'
    cm = {}
    timeout = 5
    wait = 0.2
    time_between_clean = 10
    time_keep_alive = 600

    clean_active: bool = False

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.cm = {}
        self.clean_active = True
        threading.Timer(1, self._clean).start()

    def _clean(self):
        logger.debug(f'Check if connexion is alive ({self.cm.__len__()} open connexions)')
        for key, value in list(self.cm.items()):
            tt = time.time()
            if 'ready' == self.cm[key]['status']:
                close_flag = False
                if tt - self.cm[key]['t_get'] > self.time_keep_alive:
                    logger.debug(
                        f'Close connexion {tt - self.cm[key]["t_get"]} > {self.time_keep_alive}'
                    )
                    self.cm[key]['close']()
                    close_flag = True
                elif self.cm[key]['alive']:
                    logger.debug('Clean connexion not alive')
                    if not self.cm[key]['alive']():
                        close_flag = True

                if close_flag:
                    c = self.cm[key]
                    del self.cm[key]
                    if c['close']:
                        logger.debug('Close connexion call')
                        c['close']()
                    else:
                        logger.warning('Close connexion needed but no close method defined')
        if len(self.cm) > 0:
            self.clean_active = True
            threading.Timer(self.time_between_clean, self._clean).start()
        else:
            self.clean_active = False

    def _create(self, identifier: str, connect_method, alive_method, close_method):
        self.cm[identifier] = {
            'status': 'in_progress',
            't_start': time.time(),
        }
        c = connect_method()
        self.cm[identifier] = {
            'status': 'ready',
            't_ready': time.time(),
            't_get': time.time(),
            'alive': alive_method,
            'close': close_method,
            'connection': c,
        }
        if not self.clean_active:
            self.clean_active = True
            threading.Timer(self.time_between_clean, self._clean).start()
        return self.cm[identifier]['connection']

    def _get_wait(self, identifier: str, retry_time: float):
        if 'in_progress' == self.cm[identifier]['status']:
            logger.debug('Connection is in progress')
            time.sleep(self.wait)
            if self.wait + retry_time < self.timeout:
                return self._get_wait(identifier, self.wait + retry_time)
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
            return self._get_wait(identifier, 0)
        else:
            logger.debug('Connection not exist, create and save it')
            return self._create(identifier, connect_method, alive_method, close_method)

    def force_clean(self):
        for key, value in list(self.cm.items()):
            if 'ready' == self.cm[key]['status']:
                c = self.cm[key]
                del self.cm[key]
                if c['close']:
                    logger.debug('Close connexion call')
                    c['close']()
                else:
                    logger.warning('Close connexion needed but no close method defined')
            else:
                del self.cm[key]
