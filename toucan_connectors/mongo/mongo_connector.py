from functools import _lru_cache_wrapper, lru_cache
from typing import Optional, Pattern, Union
from urllib.parse import quote_plus

import pandas as pd
import pymongo
from bson.regex import Regex
from bson.son import SON
from cached_property import cached_property
from jq import jq
from pydantic import create_model, validator

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.mongo.mongo_translator import MongoExpression
from toucan_connectors.toucan_connector import (
    DataSlice,
    ToucanConnector,
    ToucanDataSource,
    decorate_func_with_retry,
    strlist_to_enum,
)


def normalize_query(query, parameters):
    query = nosql_apply_parameters_to_query(query, parameters)

    if isinstance(query, dict):
        query = [{'$match': query}]

    for stage in query:
        # Allow ordered sorts
        if '$sort' in stage and isinstance(stage['$sort'], list):
            stage['$sort'] = SON([x.popitem() for x in stage['$sort']])

    return query


def apply_permissions(query, permissions):
    if permissions:
        permissions = MongoExpression().parse(permissions)
        if isinstance(query, dict):
            query = {'$and': [query, permissions]}
        else:
            query.append({'$match': permissions})
    return query


def validate_database(client, database: str):
    if database not in client.list_database_names():
        raise UnkwownMongoDatabase(f'Database {database!r} doesn\'t exist')


def validate_collection(client, database: str, collection: str):
    if collection not in client[database].list_collection_names():
        raise UnkwownMongoCollection(f'Collection {collection!r} doesn\'t exist')


class MongoDataSource(ToucanDataSource):
    """Supports simple, multiples and aggregation queries as desribed in
     [our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)"""

    database: str
    collection: str
    query: Union[dict, list] = {}

    @classmethod
    def get_form(cls, connector: 'MongoConnector', current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `collection` field
        """
        client = pymongo.MongoClient(connector.uri, ssl=connector.ssl)

        # Add constraints to the schema
        # the key has to be a valid field
        # the value is either <default value> or a tuple ( <type>, <default value> )
        # If the field is required, the <default value> has to be '...' (cf pydantic doc)
        constraints = {}

        # Always add the suggestions for the available databases
        available_databases = client.list_database_names()
        constraints['database'] = strlist_to_enum('database', available_databases)

        if 'database' in current_config:
            validate_database(client, current_config['database'])
            available_cols = client[current_config['database']].list_collection_names()
            constraints['collection'] = strlist_to_enum('collection', available_cols)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class MongoConnector(ToucanConnector):
    """ Retrieve data from a [MongoDB](https://www.mongodb.com/) database."""

    data_source_model: MongoDataSource

    host: str
    port: int
    username: str = None
    password: str = None
    ssl: bool = False

    class Config:
        keep_untouched = (cached_property, _lru_cache_wrapper)

    @validator('password')
    def password_must_have_a_user(cls, v, values):
        if values['username'] is None:
            raise ValueError('username must be set')
        return v

    @property
    def uri(self) -> str:
        user_pass = ''
        if self.username is not None:
            user_pass = quote_plus(self.username)
            if self.password is not None:
                user_pass += f':{quote_plus(self.password)}'
            user_pass += '@'
        return f'mongodb://{user_pass}{self.host}:{self.port}'

    def __hash__(self):
        return hash(id(self)) + hash(self.uri)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = ['Hostname resolved', 'Port opened', 'Host connection', 'Authenticated']
        ok_checks = [(c, True) for i, c in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(c, None) for i, c in enumerate(checks) if i > index]
        return ok_checks + [new_check] + not_validated_checks

    def get_status(self):
        # Check hostname
        try:
            self.check_hostname(self.host)
        except Exception as e:
            return {'status': False, 'details': self._get_details(0, False), 'error': str(e)}

        # Check port
        try:
            self.check_port(self.host, self.port)
        except Exception as e:
            return {'status': False, 'details': self._get_details(1, False), 'error': str(e)}

        # Check databases access
        client = pymongo.MongoClient(self.uri, ssl=self.ssl, serverSelectionTimeoutMS=500)
        try:
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as e:
            return {'status': False, 'details': self._get_details(2, False), 'error': str(e)}
        except pymongo.errors.OperationFailure as e:
            return {'status': False, 'details': self._get_details(3, False), 'error': str(e)}

        return {'status': True, 'details': self._get_details(3, True), 'error': None}

    @cached_property
    def client(self):
        return pymongo.MongoClient(self.uri, ssl=self.ssl)

    @lru_cache(maxsize=32)
    def validate_database(self, database: str):
        return validate_database(self.client, database)

    @lru_cache(maxsize=32)
    def validate_collection(self, database: str, collection: str):
        return validate_collection(self.client, database, collection)

    def validate_database_and_collection(self, database: str, collection: str):
        self.validate_database(database)
        self.validate_collection(database, collection)

    def _execute_query(self, data_source: MongoDataSource):
        self.validate_database_and_collection(data_source.database, data_source.collection)
        col = self.client[data_source.database][data_source.collection]
        return col.aggregate(data_source.query)

    def _retrieve_data(self, data_source):
        data_source.query = normalize_query(data_source.query, data_source.parameters)
        data = self._execute_query(data_source)
        return pd.DataFrame(list(data))

    @decorate_func_with_retry
    def get_df(self, data_source, permissions=None):
        data_source.query = apply_permissions(data_source.query, permissions)
        return self._retrieve_data(data_source)

    @decorate_func_with_retry
    def get_slice(
        self,
        data_source: MongoDataSource,
        permissions: Optional[str] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> DataSlice:
        # Create a copy in order to keep the original (deepcopy-like)
        data_source = MongoDataSource.parse_obj(data_source)
        if offset or limit is not None:
            data_source.query = apply_permissions(data_source.query, permissions)
            data_source.query = normalize_query(data_source.query, data_source.parameters)
            facet = {"$facet": {'count': data_source.query.copy(), 'df': data_source.query.copy()}}
            facet['$facet']['count'].append({'$count': 'value'})
            if offset:
                facet['$facet']['df'].append({'$skip': offset})
            if limit is not None:
                facet['$facet']['df'].append({'$limit': limit})
            data_source.query = [facet]
            res = self._execute_query(data_source).next()
            total_count = res['count'][0]['value'] if len(res['count']) > 0 else 0
            df = pd.DataFrame(res['df'])
        else:
            df = self.get_df(data_source, permissions)
            total_count = len(df)
        return DataSlice(df, total_count)

    def get_df_with_regex(
        self,
        data_source: MongoDataSource,
        field: str,
        regex: Pattern,
        permissions: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        # Create a copy in order to keep the original (deepcopy-like)
        data_source = MongoDataSource.parse_obj(data_source)
        data_source.query = normalize_query(data_source.query, data_source.parameters)
        data_source.query[0]['$match'] = {
            '$and': [data_source.query[0]['$match']]
            + [{field: {'$regex': Regex.from_native(regex)}}]
        }
        return self.get_slice(data_source, permissions, limit=limit).df

    @decorate_func_with_retry
    def explain(self, data_source, permissions=None):
        client = pymongo.MongoClient(self.uri, ssl=self.ssl)
        self.validate_database_and_collection(data_source.database, data_source.collection)
        data_source.query = apply_permissions(data_source.query, permissions)
        data_source.query = normalize_query(data_source.query, data_source.parameters)

        cursor = client[data_source.database].command(
            command="aggregate",
            value=data_source.collection,
            pipeline=data_source.query,
            explain=True,
        )

        f = '''{
                    details: (. | del(.serverInfo)),
                    summary: (.executionStats | del(.executionStages, .allPlansExecution))
                }'''
        client.close()

        return jq(f).transform(cursor)


class UnkwownMongoDatabase(Exception):
    """raised when a database does not exist"""


class UnkwownMongoCollection(Exception):
    """raised when a collection is not in the database"""
