from functools import singledispatch
import re
from typing import Union
from urllib.parse import quote_plus

import pandas as pd
import pymongo
from pydantic import validator

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import nosql_apply_parameters_to_query


PARAM_PATTERN = r'%\(\w*\)s'


@singledispatch
def handle_missing_params(d, params):
    """
    Remove a dictionary key if its value has a missing parameter.
    This is used to support the __VOID__ syntax, which is specific to
    the use of mongo at Toucan Toco : cf. https://bit.ly/2Ln6rcf
    """
    e = {}
    for k, v in d.items():
        if isinstance(v, str):
            matches = re.findall(PARAM_PATTERN, v)
            missing_params = [m[2:-2] not in params.keys() for m in matches]
            if any(missing_params):
                continue
            else:
                e[k] = v
        elif isinstance(v, dict) or isinstance(v, list):
            e[k] = handle_missing_params(v, params)
        else:
            e[k] = v
    return e


@handle_missing_params.register(list)
def handle_multiple_steps(l, params):
    """
    Handle missing parameters in multiple queries and aggregations
    """
    return [handle_missing_params(e, params) for e in l]


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

        col = client[self.database][data_source.collection]

        if isinstance(data_source.query, str):
            data_source.query = {'domain': data_source.query}
        data_source.query = handle_missing_params(data_source.query, data_source.parameters)
        data_source.query = nosql_apply_parameters_to_query(data_source.query,
                                                            data_source.parameters)
        data = []
        if isinstance(data_source.query, dict):
            data = col.find(data_source.query)
        elif isinstance(data_source.query, list):
            data = col.aggregate(data_source.query)
        df = pd.DataFrame(list(data))

        client.close()
        return df
