from typing import Union
from urllib.parse import quote_plus

import pandas as pd
import pymongo
from pydantic import validator

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import apply_parameters_to_query


class MongoDataSource(ToucanDataSource):
    """Supports simple, multiples and aggregation queries as desribed in
     [our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)"""
    collection: str
    query: Union[str, dict, list]
    parameters: dict = None


class MongoConnector(ToucanConnector):
    """ Retrieve data from a [MongoDB](https://www.mongodb.com/) database."""
    type = 'MongoDB'
    data_source_model: MongoDataSource

    host: str
    port: int
    database: str
    username: str = None
    password: str = None

    @validator('password')
    def password_must_have_a_user(cls, v, values, **kwargs):
        if values['username'] is None:
            raise ValueError('username must be set')
        return v

    @property
    def uri(self):
        user_pass = ''
        if self.username is not None:
            user_pass = quote_plus(self.username)
            if self.password is not None:
                user_pass += f':{quote_plus(self.password)}'
            user_pass += '@'
        return ''.join(['mongodb://', user_pass, f'{self.host}:{self.port}'])

    def get_df(self, data_source):
        client = pymongo.MongoClient(self.uri)

        cursor = client[self.database][data_source.collection]
        data = None
        data_source.query = apply_parameters_to_query(data_source.query,
                                                      data_source.parameters)
        if isinstance(data_source.query, str):
            data = cursor.find({'domain': data_source.query})
        elif isinstance(data_source.query, dict):
            data = cursor.find(data_source.query)
        elif isinstance(data_source.query, list):
            data = cursor.aggregate(data_source.query)
        df = pd.DataFrame(list(data))

        client.close()
        return df
