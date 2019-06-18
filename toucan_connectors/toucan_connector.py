import logging
import operator
import socket
from abc import ABCMeta, abstractmethod
from functools import reduce
from typing import Iterable, List, Optional, Tuple, Type, Union

import pandas as pd
import tenacity as tny
from pydantic import BaseModel


class ToucanDataSource(BaseModel):
    domain: str
    name: str
    type: str = None
    load: bool = True
    live_data: bool = False
    validation: list = None

    class Config:
        extra = 'forbid'
        validate_assignment = True


class RetryPolicy(BaseModel):
    """Generic "retry" policy management.

    This is just a declarative wrapper around `tenacity.retry` that should
    ease retry policy definition for most classic use cases.

    It can be instantiated with the following parameters:

    - `retry_on`: the list of expected exception classes that should trigger a retry
    - `max_attempts`: the maximum number of retries before giving up
    - `max_delay`: delay, in seconds, above which we should give up
    - `wait_time`: time, in seconds, between each retry.

    The class also exposes the `retry_decorator` method that is responsible to convert
    the parameters aforementioned in a corresponding `tenacity.retry` decorator. If
    you need a really custom retry policy and want to use the full power of the
    `tenacity` library, you can simply override this method and return your own
    `tenacity.retry` decorator.
    """
    # retry_on: Iterable[BaseException] = ()
    max_attempts: Optional[int] = 1
    max_delay: Optional[float] = 0.
    wait_time: Optional[float] = 0.

    def __init__(self, retry_on=(), logger=None, **data):
        super().__init__(**data)
        self.__dict__['retry_on'] = retry_on
        self.__dict__['logger'] = logger

    @property
    def tny_stop(self):
        """generate corresponding `stop` parameter for `tenacity.retry`"""
        stoppers = []
        if self.max_attempts > 1:
            stoppers.append(tny.stop_after_attempt(self.max_attempts))
        if self.max_delay:
            stoppers.append(tny.stop_after_delay(self.max_delay))
        if stoppers:
            return reduce(operator.or_, stoppers)
        return None

    @property
    def tny_retry(self):
        """generate corresponding `retry` parameter for `tenacity.retry`"""
        if self.retry_on:
            return tny.retry_if_exception_type(self.retry_on)
        return None

    @property
    def tny_wait(self):
        """generate corresponding `wait` parameter for `tenacity.retry`"""
        if self.wait_time:
            return tny.wait_fixed(self.wait_time)
        return None

    @property
    def tny_after(self):
        """generate corresponding `after` parameter for `tenacity.retry`"""
        if self.logger:
            return tny.after_log(self.logger, logging.DEBUG)
        return None

    def retry_decorator(self):
        """build the `tenaticy.retry` decorator corresponding to policy"""
        tny_kwargs = {}
        for attr in dir(self):
            # the "after" hook is handled separately later to plug it only if
            # there is an actual retry policy
            if attr.startswith('tny_') and attr != 'tny_after':
                paramvalue = getattr(self, attr)
                if paramvalue is not None:
                    tny_kwargs[attr[4:]] = paramvalue
        if tny_kwargs:
            # plug the "after" hook if there's one
            if self.tny_after:
                tny_kwargs['after'] = self.tny_after
            return tny.retry(reraise=True, **tny_kwargs)
        return None

    def __call__(self, f):
        """make retry_policy instances behave as `tenacity.retry` decorators"""
        decorator = self.retry_decorator()
        if decorator:
            return decorator(f)
        return f


def decorate_get_df_with_retry(get_df):
    """wrap `get_df` with the retry policy defined on the connector.

    If the retry policy is None, just leave the `get_df` implementation as is.
    """
    def get_df_and_retry(self: ToucanConnector, data_source: ToucanDataSource) -> pd.DataFrame:
        if self.retry_decorator:
            return self.retry_decorator(get_df)(self, data_source)
        else:
            return get_df(self, data_source)
    return get_df_and_retry


class ToucanConnector(BaseModel, metaclass=ABCMeta):
    """Abstract base class for all toucan connectors.

    Each concrete connector should implement the `get_df` method that accepts a
    datasource definition and return the corresponding pandas dataframe. This
    base class allows to specify a retry policy on the `get_df` method. The
    default is not to retry on error but you can customize some of connector
    model parameters to define custom retry policy.

    Model parameters:


    - `max_attempts`: the maximum number of retries before giving up
    - `max_delay`: delay, in seconds, above which we should give up
    - `wait_time`: time, in seconds, between each retry.

    In order to retry only on some custom exception classes, you can override
    the `_retry_on` class attribute in your concrete connector class.
    """
    name: str
    retry_policy: Optional[RetryPolicy] = RetryPolicy()
    _retry_on: Iterable[Type[BaseException]] = ()

    class Config:
        extra = 'forbid'
        validate_assignment = True

    def __init_subclass__(cls):
        try:
            cls.type = cls.__fields__['type'].default
            cls.data_source_model = cls.__fields__.pop('data_source_model').type_
            # only wrap get_df if the class actually implements it
            if 'get_df' in cls.__dict__:
                cls.get_df = decorate_get_df_with_retry(cls.get_df)
            cls.logger = logging.getLogger(cls.__name__)
        except KeyError as e:
            raise TypeError(f'{cls.__name__} has no {e} attribute.')

    @property
    def retry_decorator(self):
        return RetryPolicy(**self.retry_policy.dict(),
                           retry_on=self._retry_on,
                           logger=self.logger)

    @abstractmethod
    def get_df(self, data_source: ToucanDataSource) -> pd.DataFrame:
        """Main method to retrieve a pandas dataframe"""

    def get_df_and_count(self, data_source: ToucanDataSource, limit: Union[int, None]) -> dict:
        """
        Method to retrieve a part of the data as a pandas dataframe
        and the total size
        """
        df = self.get_df(data_source)
        count = len(df)
        return {'df': df[:limit], 'count': count}

    def explain(self, data_source: ToucanDataSource):
        """Method to give metrics about the query"""
        return None

    @staticmethod
    def check_hostname(hostname) -> bool:
        """Check if a hostname is resolved"""
        try:
            socket.gethostbyname(hostname)
            return True
        except socket.error:
            return False

    @staticmethod
    def check_port(host, port) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            result = s.connect_ex((host, port))
            return result == 0

    def get_status(self) -> List[Tuple[str, Optional[bool]]]:
        """
        Check if connection can be made.
        Returns [ (<test message>, <status>) ]
        e.g. [
          ('hostname resolved', True),
          ('port opened', False),
          ('db validation', None),
          ...
        ]
        """
        return [('connection status', None)]
