import inspect
import logging
from abc import ABCMeta, abstractmethod


class AbstractConnector(metaclass=ABCMeta):
    """
    Mandatory parameters: name, mandatory
    Optional parameters: optional, host
    """
    logger = logging.getLogger(__name__)

    def __new__(cls, *args, **kwargs):
        spec = inspect.getfullargspec(cls.__init__)
        if len(spec.args) > 1:
            raise BadSignature('All parameters must be keywords only (self, *, host, ...)')
        if spec.varkw:
            raise BadSignature('All parameters must be explicitly named (**kwargs forbidden)')
        mandatory_params = [p for p in spec.kwonlyargs if p not in spec.kwonlydefaults]
        model = f'mandary parameters: {mandatory_params}, optional: {spec.kwonlydefaults}'
        if any(p not in kwargs for p in mandatory_params):
            raise BadParameters(f'Missing parameters for {cls.__name__} ({model})')
        if any(p not in spec.kwonlyargs for p in kwargs):
            raise BadParameters(f'Too many parameters for {cls.__name__} ({model})')
        return super().__new__(cls)

    def __init_subclass__(cls, type, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.type = type

    def __enter__(self):
        try:
            self.connect()
            self.logger.info('Connection opened')
            return self
        except Exception as e:
            self.logger.error(f'open connection error: {e}')
            raise UnableToConnectToDatabaseException(e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        self.logger.info('Connection closed')

    @abstractmethod
    def connect(self):
        """ Method to connect to the server """

    @abstractmethod
    def disconnect(self):
        """ Method to disconnect from the server """

    def _query(self, *args, **kwargs):
        """ Method to run a query and fetch some data """

    def query(self, *args, **kwargs):
        with self:
            try:
                return self._query(*args, **kwargs)
            except Exception as e:
                self.logger.error(f'Query error: {e}')
                raise InvalidQuery(e)

    @abstractmethod
    def _get_df(self, config):
        """ Method to get a pandas dataframe """

    def get_df(self, config):
        with self:
            return self._get_df(config)


class BadSignature(Exception):
    """ Raised when a connector has a bad __init__ method """


class BadParameters(Exception):
    """ Raised when we try to create a connector with bad parameters """


class UnableToConnectToDatabaseException(Exception):
    """ Raised when it fails to connect """


class InvalidQuery(Exception):
    """ Raised when it fails to query """


class MissingQueryParameter(Exception):
    """ Can be raised if a query has some mandory parameters """
