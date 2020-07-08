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
from toucan_connectors.documentdb.documentdb_translator import DocumentDBConditionTranslator
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
        raise UnkwownDocumentDBDatabase(f"Database {database!r} doesn't exist")


def validate_collection(client, database: str, collection: str):
    if collection not in client[database].list_collection_names():
        raise UnkwownDocumentDBCollection(f"Collection {collection!r} doesn't exist")


class DocumentDBDataSource(ToucanDataSource):
    """Supports simple, multiples and aggregation queries as described in
     [our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)"""

    database: str = Field(..., description='The name of the database you want to query')
    collection: str = Field(..., description='The name of the collection you want to query')
    query: Union[dict, list] = Field(
        {},
        description='A documentdb query. See more details on the DocumentDB '
        'Aggregation Pipeline in the DocumentDB documentation',
    )

    @classmethod
    def get_form(cls, connector: 'DocumentDBConnector', current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `collection` field
        """
        client = pymongo.MongoClient(**connector._get_documentdb_client_kwargs())

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


class DocumentDBConnector(ToucanConnector):
    """ Retrieve data from a [DocumentDB](https://aws.amazon.com/documentdb/) database."""

    data_source_model: DocumentDBDataSource

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
        return hash(id(self)) + hash(json.dumps(self._get_documentdb_client_kwargs()))

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

    def _get_documentdb_client_kwargs(self):
        # We don't want parent class attributes nor the `client` property
        # nor attributes with `None` value
        to_exclude = set(ToucanConnector.__fields__) | {'client'}
        documentdb_client_kwargs = self.dict(exclude=to_exclude, exclude_none=True).copy()

        if 'password' in documentdb_client_kwargs:
            documentdb_client_kwargs['password'] = documentdb_client_kwargs['password'].get_secret_value()

        return documentdb_client_kwargs

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
        documentdb_client_kwargs = self._get_documentdb_client_kwargs()
        documentdb_client_kwargs['serverSelectionTimeoutMS'] = 500
        client = pymongo.MongoClient(**documentdb_client_kwargs)
        try:
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as e:
            return {'status': False, 'details': self._get_details(2, False), 'error': str(e)}
        except pymongo.errors.OperationFailure as e:
            return {'status': False, 'details': self._get_details(3, False), 'error': str(e)}

        return {'status': True, 'details': self._get_details(3, True), 'error': None}

    @cached_property
    def client(self):
        return pymongo.MongoClient(**self._get_documentdb_client_kwargs())

    @lru_cache(maxsize=32)
    def validate_database(self, database: str):
        return validate_database(self.client, database)

    @lru_cache(maxsize=32)
    def validate_collection(self, database: str, collection: str):
        return validate_collection(self.client, database, collection)

    def validate_database_and_collection(self, database: str, collection: str):
        self.validate_database(database)
        self.validate_collection(database, collection)

    def _execute_query(self, data_source: DocumentDBDataSource):
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
        data_source: DocumentDBDataSource,
        permissions: Optional[str] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> DataSlice:
        # Create a copy in order to keep the original (deepcopy-like)
        data_source = DocumentDBDataSource.parse_obj(data_source)
        if offset or limit is not None:
            data_source.query = apply_permissions(data_source.query, permissions)
            data_source.query = normalize_query(data_source.query, data_source.parameters)

            data_source.query = apply_permissions(data_source.query, permissions)
            data_source.query = normalize_query(data_source.query, data_source.parameters)

            group = {
                "$group": {
                    "_id": None,
                    "count": {
                        "$sum": 1
                    },
                    "df" : {"$push" : "$$ROOT"}
                }
            }
            lookup = {
                "$lookup": {
                    "from": data_source.database,
                    "localField": "tmp",
                    "foreignField": "tmp",
                    "as": "df"
                    }
            }
            limit_q = "$count"
            if limit is not None:
                limit_q = limit
            project = {
                "$project": {
                    "_id": None,
                    "count": 1, 
                    "df": {
                        "$slice": ["$df", offset,  limit_q]
                    }           
                }
            }
            data_source.query.append(group)
            data_source.query.append(lookup)
            data_source.query.append(project)
            res = self._execute_query(data_source).next()
            total_count = res['count'][0]['value'] if len(res['count']) > 0 else 0
            df = pd.DataFrame(res['df'])
        else:
            df = self.get_df(data_source, permissions)
            total_count = len(df)
        return DataSlice(df, total_count)

    def get_df_with_regex(
        self,
        data_source: DocumentDBDataSource,
        field: str,
        regex: Pattern,
        permissions: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        # Create a copy in order to keep the original (deepcopy-like)
        data_source = DocumentDBDataSource.parse_obj(data_source)
        data_source.query = normalize_query(data_source.query, data_source.parameters)
        data_source.query[0]['$match'] = {
            '$and': [data_source.query[0]['$match']]
            + [{field: {'$regex': Regex.from_native(regex)}}]
        }
        return self.get_slice(data_source, permissions, limit=limit).df

    @decorate_func_with_retry
    def explain(self, data_source, permissions=None):
        client = pymongo.MongoClient(**self._get_documentdb_client_kwargs())
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
    """format output of an `explain` documentdb command
    Return a dictionary with 2 properties:
    - 'details': the origin explain result without the `serverInfo` part
      to avoid leaing documentdb server version number
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


class UnkwownDocumentDBDatabase(Exception):
    """raised when a database does not exist"""


class UnkwownDocumentDBCollection(Exception):
    """raised when a collection is not in the database"""
