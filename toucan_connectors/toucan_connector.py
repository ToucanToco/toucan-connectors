import logging
from abc import ABCMeta, abstractmethod

import pandas as pd
from pydantic import BaseModel


class ToucanDataSource(BaseModel):
    domain: str
    name: str
    type: str = None
    load: bool = True
    live_data: bool = False

    class Config:
        ignore_extra = False
        validate_assignment = True


class ToucanConnector(BaseModel, metaclass=ABCMeta):
    name: str

    class Config:
        ignore_extra = False
        validate_assignment = True

    def __init_subclass__(cls):
        try:
            cls.type = cls.__fields__['type'].default
            cls.data_source_model = cls.__fields__.pop('data_source_model').type_
            cls.logger = logging.getLogger(cls.__name__)
        except KeyError as e:
            raise TypeError(f'{cls.__name__} has no {e} attribute.')

    @abstractmethod
    def get_df(self, data_source: ToucanDataSource) -> pd.DataFrame:
        """Main method to retrieve a pandas dataframe"""
