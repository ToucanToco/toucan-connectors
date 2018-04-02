import abc

import pandas as pd
from pydantic import BaseModel


class ToucanDataSource(BaseModel):
    domain: str
    name: str

    class Config:
        ignore_extra = False
        validate_assignment = True


class ToucanConnector(BaseModel, metaclass=abc.ABCMeta):
    name: str

    class Config:
        ignore_extra = False
        validate_assignment = True

    def __init_subclass__(cls):
        try:
            cls.type = cls.__fields__['type'].default
        except KeyError:
            raise TypeError(f'{cls.__name__} has no type attribute.')

    @abc.abstractmethod
    def get_df(self, data_source) -> pd.DataFrame:
        pass
