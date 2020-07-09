import json
from functools import _lru_cache_wrapper, lru_cache
from typing import Optional, Pattern, Union

import pandas as pd
import pymongo
from bson.regex import Regex
from bson.son import SON
from cached_property import cached_property
from pydantic import Field, SecretStr, create_model, validator

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.mongo.mongo_translator import MongoConditionTranslator
from toucan_connectors.toucan_connector import (
    DataSlice,
    ToucanConnector,
    ToucanDataSource,
    decorate_func_with_retry,
    strlist_to_enum,
)

MAX_COUNTED_ROWS = 1000001


def normalize_query(query, parameters):
    query = nosql_apply_parameters_to_query(query, parameters)

    if isinstance(query, dict):
        query = [{'$match': query}]

    for stage in query:
        # Allow ordered sorts
        if '$sort' in stage and isinstance(stage['$sort'], list):
            stage['$sort'] = SON([x.popitem() for x in stage['$sort']])

    return query


def apply_permissions(query, permissions_condition: dict):
    if permissions_condition:
        permissions = MongoConditionTranslator.translate(permissions_condition)
        if isinstance(query, dict):
            query = {'$and': [query, permissions]}
        else:
            query[0]['$match'] = {'$and': [query[0]['$match'], permissions]}
    return query


def validate_database(client, database: str):
    if database not in client.list_database_names():
        raise UnkwownMongoDatabase(f"Database {database!r} doesn't exist")


def validate_collection(client, database: str, collection: str):
    if collection not in client[database].list_collection_names():
        raise UnkwownMongoCollection(f"Collection {collection!r} doesn't exist")


class MongoDataSource(ToucanDataSource):
    """Supports simple, multiples and aggregation queries as described in
     [our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)"""

    database: str = Field(..., description='The name of the database you want to query')
    collection: str = Field(..., description='The name of the collection you want to query')
    query: Union[dict, list] = Field(
        {},
        description='A mongo query. See more details on the Mongo '
        'Aggregation Pipeline in the MongoDB documentation',
    )

    @classmethod
    def get_form(cls, connector: 'MongoConnector', current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `collection` field
        """
        client = pymongo.MongoClient(**connector._get_mongo_client_kwargs())

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

    host: str = Field(
        ...,
        description='The domain name (preferred option as more dynamic) or '
        'the hardcoded IP address of your database server',
    )
    port: Optional[int] = Field(None, description='The listening port of your database server')
    username: Optional[str] = Field(None, description='Your login username')
    password: Optional[SecretStr] = Field(None, description='Your login password')
    ssl: Optional[bool] = Field(None, description='Create the connection to the server using SSL')

    class Config:
        keep_untouched = (cached_property, _lru_cache_wrapper)

    @validator('password')
    def password_must_have_a_user(cls, password, values):
        if password is not None and values['username'] is None:
            raise ValueError('username must be set')
        return password

    def __hash__(self):
        return hash(id(self)) + hash(json.dumps(self._get_mongo_client_kwargs()))

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

    def _get_mongo_client_kwargs(self):
        # We don't want parent class attributes nor the `client` property
        # nor attributes with `None` value
        to_exclude = set(ToucanConnector.__fields__) | {'client'}
        mongo_client_kwargs = self.dict(exclude=to_exclude, exclude_none=True).copy()

        if 'password' in mongo_client_kwargs:
            mongo_client_kwargs['password'] = mongo_client_kwargs['password'].get_secret_value()

        return mongo_client_kwargs

    def get_status(self):
        if self.port:
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
        mongo_client_kwargs = self._get_mongo_client_kwargs()
        mongo_client_kwargs['serverSelectionTimeoutMS'] = 500
        client = pymongo.MongoClient(**mongo_client_kwargs)
        try:
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as e:
            return {'status': False, 'details': self._get_details(2, False), 'error': str(e)}
        except pymongo.errors.OperationFailure as e:
            return {'status': False, 'details': self._get_details(3, False), 'error': str(e)}

        return {'status': True, 'details': self._get_details(3, True), 'error': None}

    @cached_property
    def client(self):
        return pymongo.MongoClient(**self._get_mongo_client_kwargs())

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

            df_facet = []
            if offset:
                df_facet.append({'$skip': offset})
            if limit is not None:
                df_facet.append({'$limit': limit})
            facet = {
                '$facet': {
                    # counting more than 1M values can be really slow, and the exact number is not that much relevant
                    'count': [{'$limit': MAX_COUNTED_ROWS}, {'$count': 'value'}],
                    'df': df_facet,  # df_facet is never empty
                }
            }
            data_source.query.append(facet)

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
        client = pymongo.MongoClient(**self._get_mongo_client_kwargs())
        self.validate_database_and_collection(data_source.database, data_source.collection)
        data_source.query = apply_permissions(data_source.query, permissions)
        data_source.query = normalize_query(data_source.query, data_source.parameters)

        agg_cmd = SON(
            [
                ('aggregate', data_source.collection),
                ('pipeline', data_source.query),
                ('cursor', {}),
            ]
        )
        result = client[data_source.database].command(
            command='explain', value=agg_cmd, verbosity='executionStats'
        )
        return _format_explain_result(result)


def _format_explain_result(explain_result):
    """format output of an `explain` mongo command

    Return a dictionary with 2 properties:

    - 'details': the origin explain result without the `serverInfo` part
      to avoid leaing mongo server version number
    - 'summary': the list of execution statistics (i.e. drop the details of
       candidate plans)

    if `explain_result` is empty, return `None`
    """
    if explain_result:
        explain_result.pop('serverInfo', None)
        if 'stages' in explain_result:
            stats = [
                stage['$cursor']['executionStats']
                for stage in explain_result['stages']
                if '$cursor' in stage
            ]
        else:
            stats = [explain_result['executionStats']]
        return {
            'details': explain_result,
            'summary': stats,
        }
    return None


class UnkwownMongoDatabase(Exception):
    """raised when a database does not exist"""


class UnkwownMongoCollection(Exception):
    """raised when a collection is not in the database"""
