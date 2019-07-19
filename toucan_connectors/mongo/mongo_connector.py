from typing import Optional, Union
from urllib.parse import quote_plus

import pandas as pd
import pymongo
from bson.son import SON
from jq import jq
from pydantic import validator

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.mongo.mongo_translator import MongoExpression
from toucan_connectors.toucan_connector import ToucanConnector, \
    ToucanDataSource, decorate_func_with_retry


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


class MongoDataSource(ToucanDataSource):
    """Supports simple, multiples and aggregation queries as desribed in
     [our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)"""
    database: str
    collection: str
    query: Union[dict, list] = {}


class MongoConnector(ToucanConnector):
    """ Retrieve data from a [MongoDB](https://www.mongodb.com/) database."""
    data_source_model: MongoDataSource

    host: str
    port: int
    username: str = None
    password: str = None
    ssl: bool = False

    @validator('password')
    def password_must_have_a_user(cls, v, values):
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

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = [
            'Hostname resolved',
            'Port opened',
            'Host connection',
            'Authenticated',
        ]
        ok_checks = [(c, True) for i, c in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(c, None) for i, c in enumerate(checks) if i > index]
        return ok_checks + [new_check] + not_validated_checks

    def get_status(self):
        # Check hostname
        try:
            self.check_hostname(self.host)
        except Exception as e:
            return {
                'status': False,
                'details': self._get_details(0, False),
                'error': str(e)
            }

        # Check port
        try:
            self.check_port(self.host, self.port)
        except Exception as e:
            return {
                'status': False,
                'details': self._get_details(1, False),
                'error': str(e)
            }

        # Check databases access
        client = pymongo.MongoClient(self.uri, ssl=self.ssl, serverSelectionTimeoutMS=500)
        try:
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as e:
            return {
                'status': False,
                'details': self._get_details(2, False),
                'error': str(e)
            }
        except pymongo.errors.OperationFailure as e:
            return {
                'status': False,
                'details': self._get_details(3, False),
                'error': str(e)
            }

        return {
            'status': True,
            'details': self._get_details(3, True),
            'error': None
        }

    @staticmethod
    def _validate_database_and_collection(client, database, collection):
        if database not in client.list_database_names():
            raise UnkwownMongoDatabase(f'Database {database!r} doesn\'t exist')

        if collection not in client[database].list_collection_names():
            raise UnkwownMongoCollection(f'Collection {collection!r} doesn\'t exist')

    def _execute_query(self, data_source):
        client = pymongo.MongoClient(self.uri, ssl=self.ssl)
        self._validate_database_and_collection(
            client, data_source.database, data_source.collection
        )
        col = client[data_source.database][data_source.collection]

        data_source.query = normalize_query(data_source.query,
                                            data_source.parameters)
        result = col.aggregate(data_source.query)
        client.close()
        return result

    def _retrieve_data(self, data_source):
        data = self._execute_query(data_source)
        return pd.DataFrame(list(data))

    @decorate_func_with_retry
    def get_df(self, data_source, permissions=None):
        data_source.query = apply_permissions(data_source.query, permissions)
        return self._retrieve_data(data_source)

    @decorate_func_with_retry
    def get_df_and_count(self, data_source, permissions=None, limit=None):
        data_source.query = apply_permissions(data_source.query, permissions)
        if limit is not None:
            if isinstance(data_source.query, dict):
                data_source.query = [{'$match': data_source.query}]
            facet = {"$facet": {
                'count': data_source.query.copy(),
                'df': data_source.query.copy(),
            }}
            facet['$facet']['count'].append({'$count': 'value'})
            facet['$facet']['df'].append({'$limit': limit})
            data_source.query = [facet]
            res = self._execute_query(data_source).next()
            count = res['count'][0]['value'] if len(res['count']) > 0 else 0
            df = pd.DataFrame(res['df'])
        else:
            df = self._retrieve_data(data_source)
            count = len(df)
        return {'df': df, 'count': count}

    @decorate_func_with_retry
    def explain(self, data_source, permissions=None):
        client = pymongo.MongoClient(self.uri, ssl=self.ssl)
        self._validate_database_and_collection(
            client, data_source.database, data_source.collection
        )
        data_source.query = apply_permissions(data_source.query, permissions)
        data_source.query = normalize_query(data_source.query,
                                            data_source.parameters)

        cursor = client[data_source.database].command(
            command="aggregate",
            value=data_source.collection,
            pipeline=data_source.query,
            explain=True
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
