import logging
from abc import ABCMeta, abstractmethod
from typing import Union

import pandas as pd
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


class ToucanConnector(BaseModel, metaclass=ABCMeta):
    name: str

    class Config:
        extra = 'forbid'
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
