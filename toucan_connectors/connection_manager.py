import logging
import threading
import time


class ConnectionManager:
    name = 'connection_manager'
    cm = {}
    timeout = 5
    wait = 0.2
    time_between_clean = 10
    time_keep_alive = 600

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        threading.Timer(1, self.__clean).start()

    def __clean(self):
        logging.getLogger(__name__).debug(
            f'Check if connexion is alive ({self.cm.__len__()} open connexions)'
        )
        for key, value in list(self.cm.items()):
            tt = time.time()
            if 'ready' == self.cm[key]['status']:
                close_flag = False
                if tt - self.cm[key]['t_get'] > self.time_keep_alive:
                    logging.getLogger(__name__).debug(
                        f'Close connexion {tt - self.cm[key]["t_get"]} > {self.time_keep_alive}'
                    )
                    self.cm[key]['close']()
                    close_flag = True
                elif self.cm[key]['alive']:
                    logging.getLogger(__name__).debug('Clean connexion not alive')
                    if self.cm[key]['alive']():
                        close_flag = True

                if close_flag:
                    c = self.cm[key]
                    del self.cm[key]
                    if c['close']:
                        logging.getLogger(__name__).debug('Close connexion call')
                        c['close']()
                    else:
                        logging.getLogger(__name__).warning(
                            'Close connexion needed but no close method defined'
                        )
        threading.Timer(self.time_between_clean, self.__clean).start()

    def __create(self, identifier: str, connect_method, alive_method, close_method):
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
        return self.cm[identifier]['connection']

    def __get_wait(self, identifier: str, retry_time: float):
        if 'in_progress' == self.cm[identifier]['status']:
            logging.getLogger(__name__).debug('Connection is in progress')
            time.sleep(self.wait)
            if self.wait + retry_time < self.timeout:
                return self.__get_wait(identifier, self.wait + retry_time)
            else:
                logging.getLogger(__name__).error(
                    f'Timeout - impossible to retrieve connexion {self.timeout}'
                )
                raise TimeoutError
        elif 'ready' == self.cm[identifier]['status']:
            logging.getLogger(__name__).debug('Connection is ready')
            self.cm[identifier]['t_get'] = time.time()
            return self.cm[identifier]['connection']

    def get(self, identifier: str, connect_method, alive_method, close_method):
        logging.getLogger(__name__).debug(f'Get element in Dict {identifier}')
        if identifier in self.cm:
            logging.getLogger(__name__).debug('Connection exist')
            return self.__get_wait(identifier, 0)
        else:
            logging.getLogger(__name__).debug('Connection not exist, create and save it')
            return self.__create(identifier, connect_method, alive_method, close_method)
