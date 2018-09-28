import json
import os

import pandas as pd
import pymongo
import pymongo.errors
import pytest

from toucan_connectors.mongo.mongo_connector import MongoDataSource, MongoConnector
from toucan_connectors.mongo.mongo_connector import handle_missing_params


@pytest.fixture(scope='module')
def mongo_server(service_container):
    def check_and_feed(host_port):
        client = pymongo.MongoClient(f'mongodb://ubuntu:ilovetoucan@localhost:{host_port}')
        docs_path = f'{os.path.dirname(__file__)}/fixtures/docs.json'
        with open(docs_path) as f:
            docs_json = f.read()
        docs = json.loads(docs_json)
        client['toucan']['test_col'].insert_many(docs)
        client.close()

    return service_container('mongo', check_and_feed, pymongo.errors.PyMongoError)


@pytest.fixture
def mongo_connector(mongo_server):
    return MongoConnector(name='mycon', host='localhost', database='toucan',
                          port=mongo_server['port'], username='ubuntu', password='ilovetoucan')


@pytest.fixture
def mongo_datasource():
    def f(collection, query):
        return MongoDataSource(name='mycon', domain='mydomain', collection=collection, query=query)

    return f


def test_uri():
    connector = MongoConnector(name='my_mongo_con', host='myhost', port='123', database='mydb')
    assert connector.uri == 'mongodb://myhost:123'
    connector = MongoConnector(name='my_mongo_con', host='myhost', port='123', database='mydb',
                               username='myuser')
    assert connector.uri == 'mongodb://myuser@myhost:123'
    connector = MongoConnector(name='my_mongo_con', host='myhost', port='123', database='mydb',
                               username='myuser', password='mypass')
    assert connector.uri == 'mongodb://myuser:mypass@myhost:123'
    with pytest.raises(ValueError) as exc_info:
        MongoConnector(name='my_mongo_con', host='myhost', port='123', database='mydb',
                       password='mypass')
    assert 'password:\n  username must be set' in str(exc_info.value)


def test_get_df(mocker):
    class MongoMock:
        def __init__(self, database, collection):
            self.data = {database: {collection: pymongo.collection.Collection}}

        def __getitem__(self, row):
            return self.data[row]

        def close(self):
            pass

    snock = mocker.patch('pymongo.MongoClient')
    snock.return_value = MongoMock('toucan', 'test_col')
    find = mocker.patch('pymongo.collection.Collection.find')
    aggregate = mocker.patch('pymongo.collection.Collection.aggregate')

    mongo_connector = MongoConnector(
        name='mycon', host='localhost', database='toucan', port=22,
        username='ubuntu', password='ilovetoucan'
    )

    datasource = MongoDataSource(
        name='mycon', domain='mydomain', collection='test_col',
        query={'domain': 'domain1'}
    )
    mongo_connector.get_df(datasource)

    datasource = MongoDataSource(
        name='mycon', domain='mydomain', collection='test_col', query='domain1'
    )
    mongo_connector.get_df(datasource)

    datasource = MongoDataSource(
        name='mycon', domain='mydomain', collection='test_col',
        query=[{'$match': {'domain': 'domain1'}}]
    )
    mongo_connector.get_df(datasource)

    snock.assert_called_with('mongodb://ubuntu:ilovetoucan@localhost:22')
    assert snock.call_count == 3

    find.assert_called_with({'domain': 'domain1'})
    assert find.call_count == 2

    aggregate.assert_called_once_with([{'$match': {'domain': 'domain1'}}])


def test_get_df_live(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df(datasource)
    expected = pd.DataFrame({'country': ['France', 'England', 'Germany'],
                             'language': ['French', 'English', 'German'],
                             'value': [20, 14, 17]})
    assert df.shape == (3, 5)
    assert df.columns.tolist() == ['_id', 'country', 'domain', 'language', 'value']
    assert df[['country', 'language', 'value']].equals(expected)

    datasource = mongo_datasource(collection='test_col', query='domain1')
    df2 = mongo_connector.get_df(datasource)
    assert df2.equals(df)

    datasource = mongo_datasource(collection='test_col', query=[{'$match': {'domain': 'domain1'}}])
    df2 = mongo_connector.get_df(datasource)
    assert df2.equals(df)


def test_handle_missing_param():
    params = {'city': 'Paris'}

    query = {
        'domain': 'blah',
        'country': {'$ne': '%(country)s'},
        'city': '%(city)s'
    }

    assert handle_missing_params(query, params) == {
        'domain': 'blah',
        'country': {},
        'city': '%(city)s'
    }

    query = [
        {'$match': {'country': '%(country)s', 'city': 'Test'}},
        {'$match': {'b': 1}}
    ]

    assert handle_missing_params(query, params) == [
        {'$match': {'city': 'Test'}},
        {'$match': {'b': 1}}
    ]

    query = {'code': '%(city)s_%(country)s', 'domain': 'Test'}
    assert handle_missing_params(query, params) == {'domain': 'Test'}

    query = [
        {'$match': {'country': '%(country)s', 'city': 'Test'}},
        {'$project': {'b': {'$divide': ['__VOID__', '$a']}}}
    ]

    assert handle_missing_params(query, params) == [
        {'$match': {'city': 'Test'}},
        {'$project': {'b': {'$divide': ['__VOID__', '$a']}}}
    ]
