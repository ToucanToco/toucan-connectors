import inspect
from abc import ABCMeta, abstractmethod


class AbstractConnector(metaclass=ABCMeta):
    """
    Mandatory parameters: name, mandatory
    Optional parameters: optional, host
    """

    def __new__(cls, *args, **kwargs):
        if args:
            raise BadSignature('To create a connector, you must have named parameters only')
        spec = inspect.getfullargspec(cls.__init__)
        if spec.varkw:
            raise BadSignature('All parameters must be explicitly named (kwargs forbidden)')
        if 'name' not in spec.kwonlyargs:
            raise BadSignature('"name" is a mandatory parameter')
        mandatory_params = [p for p in spec.kwonlyargs if p not in spec.kwonlydefaults]
        model = f'mandary parameters: {mandatory_params}, optional: {spec.kwonlydefaults}'
        if any(p not in kwargs for p in mandatory_params):
            raise BadParameters(f'Missing parameters for {cls.__name__} ({model})')
        if any(p not in spec.kwonlyargs for p in kwargs):
            raise BadParameters(f'Too many parameters for {cls.__name__} ({model})')
        return super().__new__(cls)

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    @abstractmethod
    def query(self):
        pass

    @abstractmethod
    def get_df(self):
        pass


class BadSignature(Exception):
    """ Raised when a connector has a bad __init__ method """


class BadParameters(Exception):
    """ Raised when we try to create a connector with bad parameters """

