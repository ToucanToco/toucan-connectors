import re
from jq import jq
from typing import Union
from urllib.parse import quote_plus

import pandas as pd
import pymongo
from bson.son import SON
from pydantic import validator

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.toucan_connector import ToucanConnector, \
    ToucanDataSource

PARAM_PATTERN = r'%\(\w*\)s'


def handle_missing_params(elt, params):
    """
    Remove a dictionary key if its value has a missing parameter.
    This is used to support the __VOID__ syntax, which is specific to
    the use of mongo at Toucan Toco : cf. https://bit.ly/2Ln6rcf
    """
    if isinstance(elt, dict):
        e = {}
        for k, v in elt.items():
            if isinstance(v, str):
                matches = re.findall(PARAM_PATTERN, v)
                missing_params = [m[2:-2] not in params.keys() for m in matches]
                if any(missing_params):
                    continue
                else:
                    e[k] = v
            else:
                e[k] = handle_missing_params(v, params)
        return e
    elif isinstance(elt, list):
        return [handle_missing_params(e, params) for e in elt]
    else:
        return elt


def normalize_query(query, parameters):
    query = handle_missing_params(query, parameters)
    query = nosql_apply_parameters_to_query(query, parameters)

    if isinstance(query, dict):
        query = [{'$match': query}]

    for stage in query:
        # Allow ordered sorts
        if '$sort' in stage and isinstance(stage['$sort'], list):
            stage['$sort'] = SON([x.popitem() for x in stage['$sort']])

    return query


class MongoDataSource(ToucanDataSource):
    """Supports simple, multiples and aggregation queries as desribed in
     [our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)"""
    collection: str
    query: Union[dict, list]
    parameters: dict = None


class MongoConnector(ToucanConnector):
    """ Retrieve data from a [MongoDB](https://www.mongodb.com/) database."""
    data_source_model: MongoDataSource

    host: str
    port: int
    database: str
    username: str = None
    password: str = None
    ssl: bool = False

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

    @staticmethod
    def _get_status(hostname_resolved, port_opened=None, host_connection=None,
                    authenticated=None, database_available=None):
        return [
            ('Hostname resolved', hostname_resolved),
            ('Port opened', port_opened),
            ('Host connection', host_connection),
            ('Authenticated', authenticated),
            ('Database available', database_available)
        ]

    def get_status(self):
        # Check hostname
        hostname_resolved = self.check_hostname(self.host)
        if not hostname_resolved:
            return self._get_status(hostname_resolved)

        # Check port
        port_opened = self.check_port(self.host, self.port)
        if not port_opened:
            return self._get_status(hostname_resolved, port_opened)

        # Check databases access
        client = pymongo.MongoClient(self.uri, ssl=self.ssl, serverSelectionTimeoutMS=500)
        try:
            client.server_info()
            host_connection = True
            authenticated = True
        except pymongo.errors.ServerSelectionTimeoutError:
            host_connection = False
            return self._get_status(hostname_resolved, port_opened, host_connection)
        except pymongo.errors.OperationFailure:
            host_connection = True
            authenticated = False
            return self._get_status(hostname_resolved, port_opened, host_connection, authenticated)

        # Check if given database actually exists
        database_available = self.database in client.list_database_names()

        return self._get_status(hostname_resolved, port_opened, host_connection,
                                authenticated, database_available)

    def validate_collection(self, client, collection):
        if collection not in client[self.database].list_collection_names():
            raise UnkwownMongoCollection(f'Collection {collection} doesn\'t exist')

    def execute_query(self, data_source):
        client = pymongo.MongoClient(self.uri, ssl=self.ssl)
        self.validate_collection(client, data_source.collection)
        col = client[self.database][data_source.collection]

        data_source.query = normalize_query(data_source.query,
                                            data_source.parameters)
        result = col.aggregate(data_source.query)
        client.close()
        return result

    def get_df(self, data_source):
        data = self.execute_query(data_source)
        df = pd.DataFrame(list(data))
        return df

    def get_df_and_count(self, data_source, limit):
        if isinstance(data_source.query, dict):
            data_source.query = [{'$match': data_source.query}]
        if limit is not None:
            facet = {"$facet": {'count': [{'$count': 'value'}]}}
            facet['$facet']['df'] = [{'$limit': limit}]
            data_source.query.append(facet)
            res = self.execute_query(data_source).next()
            count = res['count'][0]['value'] if len(res['count']) > 0 else 0
            df = pd.DataFrame(res['df'])
        else:
            df = self.get_df(data_source)
            count = len(df)
        return {'df': df, 'count': count}

    def explain(self, data_source):
        client = pymongo.MongoClient(self.uri, ssl=self.ssl)
        self.validate_collection(client, data_source.collection)

        data_source.query = normalize_query(data_source.query,
                                            data_source.parameters)

        cursor = client[self.database].command(
            command="aggregate",
            value=data_source.collection,
            pipeline=data_source.query,
            explain=True
        )

        f = '''{
                    details: (. | del(.serverInfo)),
                    summary: (.executionStats | del(.executionStages, .allPlansExecution))
                }'''
        return jq(f).transform(cursor)


class UnkwownMongoCollection(Exception):
    """raised when a collection is not in the database"""
