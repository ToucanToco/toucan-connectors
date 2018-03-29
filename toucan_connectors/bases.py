from abc import ABCMeta, abstractmethod
import logging
from typing import Any

import pandas as pd


def validate_against_annotations(cls, **kwargs):
    errors = []

    # all the required arguments are in kwargs
    for attr in cls.__annotations__:
        if attr not in kwargs and not hasattr(cls, attr):
            errors.append((attr, kwargs, 'argument is required'))

    # all the kwargs are valid
    for kwarg, value in kwargs.items():
        try:
            ty = cls.__annotations__[kwarg]
            if ty != Any and not isinstance(value, ty):
                errors.append((kwarg, value, ty, 'wrong type of argument'))
        except KeyError:
            errors.append((kwarg, 'unknown argument'))

    return errors


class ToucanBase:

    def __new__(cls, *args, **kwargs):

        for k in cls.__bases__:
            cls.__annotations__.update(k.__annotations__)

        errors = validate_against_annotations(cls, **kwargs)
        if errors:
            raise BadParameters(errors)

        return super().__new__(cls)

    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class ToucanDataSource(ToucanBase):
    domain: str
    name: str


class ToucanConnector(ToucanBase, metaclass=ABCMeta):
    logger = logging.getLogger(__name__)

    name: str

    def __new__(cls, *args, **kwargs):

        if not hasattr(cls, 'type'):
            raise TypeError('Connector has no type')

        return super().__new__(cls, *args, **kwargs)

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

    @abstractmethod
    def get_df(self, data_source: ToucanDataSource) -> pd.DataFrame:
        """ Method to get a pandas dataframe """

    def _get_df(self, data_source: ToucanDataSource) -> pd.DataFrame:
        with self:
            return self.get_df(data_source)

    @classmethod
    def validate(cls, data_source: dict):
        """Validate a data_source for this type of connector """
        try:
            cls.data_source_class(**data_source)
        except AttributeError:
            raise TypeError('Implement validate or set data_source_class attr')


class BadParameters(Exception):
    """ Raised when we try to create a connector with wrong parameters """


class UnableToConnectToDatabaseException(Exception):
    """ Raised when it fails to connect """
