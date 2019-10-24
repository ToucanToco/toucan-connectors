import json
import os
import re

import pandas as pd
import pymongo
import pymongo.errors
import pytest
from bson.son import SON

from toucan_connectors.mongo.mongo_connector import (
    MongoConnector,
    MongoDataSource,
    UnkwownMongoCollection,
    UnkwownMongoDatabase,
    normalize_query,
)


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
    return MongoConnector(
        name='mycon',
        host='localhost',
        port=mongo_server['port'],
        username='ubuntu',
        password='ilovetoucan',
    )


@pytest.fixture
def mongo_datasource():
    def f(**kwargs):
        params = {'name': 'mycon', 'domain': 'mydomain', 'database': 'toucan'}
        params.update(kwargs)
        return MongoDataSource(**params)

    return f


def test_uri():
    connector = MongoConnector(name='my_mongo_con', host='myhost', port='123')
    assert connector.uri == 'mongodb://myhost:123'
    connector = MongoConnector(name='my_mongo_con', host='myhost', port='123', username='myuser')
    assert connector.uri == 'mongodb://myuser@myhost:123'
    connector = MongoConnector(
        name='my_mongo_con', host='myhost', port='123', username='myuser', password='mypass'
    )
    assert connector.uri == 'mongodb://myuser:mypass@myhost:123'
    with pytest.raises(ValueError) as exc_info:
        MongoConnector(name='my_mongo_con', host='myhost', port='123', password='mypass')
    assert 'password\n  username must be set' in str(exc_info.value)


def test_get_df_no_query(mongo_connector, mongo_datasource):
    """It should return the whole collection by default"""
    ds = mongo_datasource(collection='test_col')
    df = mongo_connector.get_df(ds)
    assert df.shape == (3, 5)


def test_get_df(mocker):
    class DatabaseMock:
        def __init__(self, collection):
            self.collections = {collection: pymongo.collection.Collection}

        def __getitem__(self, col):
            return self.collections[col]

        def list_collection_names(self):
            return self.collections.keys()

    class MongoMock:
        def __init__(self, database, collection):
            self.data = {database: DatabaseMock(collection)}

        def __getitem__(self, row):
            return self.data[row]

        def close(self):
            pass

        def list_database_names(self):
            return self.data.keys()

    snock = mocker.patch('pymongo.MongoClient')
    snock.return_value = MongoMock('toucan', 'test_col')
    aggregate = mocker.patch('pymongo.collection.Collection.aggregate')

    mongo_connector = MongoConnector(
        name='mycon', host='localhost', port=22, username='ubuntu', password='ilovetoucan'
    )

    datasource = MongoDataSource(
        name='mycon',
        domain='mydomain',
        database='toucan',
        collection='test_col',
        query={'domain': 'domain1'},
    )
    mongo_connector.get_df(datasource)

    datasource = MongoDataSource(
        name='mycon',
        domain='mydomain',
        database='toucan',
        collection='test_col',
        query=[{'$match': {'domain': 'domain1'}}],
    )
    mongo_connector.get_df(datasource)

    snock.assert_called_with('mongodb://ubuntu:ilovetoucan@localhost:22', ssl=False)
    assert snock.call_count == 1  # client is cached

    aggregate.assert_called_with([{'$match': {'domain': 'domain1'}}])
    assert aggregate.call_count == 2


def test_get_df_live(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df(datasource)
    expected = pd.DataFrame(
        {
            'country': ['France', 'England', 'Germany'],
            'language': ['French', 'English', 'German'],
            'value': [20, 14, 17],
        }
    )
    assert df.shape == (3, 5)
    assert set(df.columns) == {'_id', 'country', 'domain', 'language', 'value'}
    assert df[['country', 'language', 'value']].equals(expected)

    datasource = mongo_datasource(
        collection='test_col', query=[{'$match': {'domain': 'domain1'}}, {"$sort": [{'pays': 1}]}]
    )
    df2 = mongo_connector.get_df(datasource)
    assert df2.equals(df)


def test_get_df_with_permissions(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df(datasource, permissions='country=="France"')
    expected = pd.DataFrame({'country': ['France'], 'language': ['French'], 'value': [20]})
    assert datasource.query == [
        {'$match': {'$and': [{'domain': 'domain1'}, {'country': 'France'}]}}
    ]
    assert df.shape == (1, 5)
    assert set(df.columns) == {'_id', 'country', 'domain', 'language', 'value'}
    assert df[['country', 'language', 'value']].equals(expected)

    datasource = mongo_datasource(collection='test_col', query=[{'$match': {'domain': 'domain1'}}])
    df = mongo_connector.get_df(datasource, permissions='country=="France"')
    expected = pd.DataFrame({'country': ['France'], 'language': ['French'], 'value': [20]})
    assert datasource.query == [
        {'$match': {'$and': [{'domain': 'domain1'}, {'country': 'France'}]}}
    ]
    assert df.shape == (1, 5)
    assert set(df.columns) == {'_id', 'country', 'domain', 'language', 'value'}
    assert df[['country', 'language', 'value']].equals(expected)


def test_get_slice(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    res = mongo_connector.get_slice(datasource)
    assert res.total_count == 3
    assert res.df.shape == (3, 5)
    assert res.df['country'].tolist() == ['France', 'England', 'Germany']

    # With a limit
    res = mongo_connector.get_slice(datasource, limit=1)
    expected = pd.DataFrame({'country': ['France'], 'language': ['French'], 'value': [20]})
    assert res.total_count == 3
    assert res.df.shape == (1, 5)
    assert res.df[['country', 'language', 'value']].equals(expected)

    # With a offset
    res = mongo_connector.get_slice(datasource, offset=1)
    assert res.total_count == 3
    assert res.df.shape == (2, 5)
    assert res.df['country'].tolist() == ['England', 'Germany']

    # With both
    res = mongo_connector.get_slice(datasource, offset=1, limit=1)
    assert res.total_count == 3
    assert res.df.shape == (1, 5)
    assert res.df.loc[0, 'country'] == 'England'


def test_get_slice_with_group_agg(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(
        collection='test_col',
        query=[
            {"$match": {'domain': 'domain1'}},
            {"$group": {"_id": {"country": "$country"}}},
            {"$project": {"pays": "$_id.country", "_id": 0}},
            {"$sort": [{'pays': 1}]},
        ],
    )
    df, count = mongo_connector.get_slice(datasource, limit=1)
    assert count == 3
    assert df.shape == (1, 1)
    assert df.iloc[0].pays in ['France', 'England', 'Germany']


def test_get_slice_no_limit(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df, count = mongo_connector.get_slice(datasource, limit=None)
    assert count == 3
    expected = pd.DataFrame(
        {
            'country': ['France', 'England', 'Germany'],
            'language': ['French', 'English', 'German'],
            'value': [20, 14, 17],
        }
    )
    assert df.shape == (3, 5)
    assert df[['country', 'language', 'value']].equals(expected)


def test_get_slice_empty(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'unknown'})
    df, count = mongo_connector.get_slice(datasource, limit=1)
    assert count == 0
    assert df.shape == (0, 0)


def test_get_df_with_regex(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df_with_regex(datasource, field='country', regex=re.compile('r.*a'))
    pd.testing.assert_series_equal(df['country'], pd.Series(['France', 'Germany'], name='country'))


def test_get_df_with_regex_with_limit(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df_with_regex(
        datasource, field='country', regex=re.compile('r.*a'), limit=1
    )
    pd.testing.assert_series_equal(df['country'], pd.Series(['France'], name='country'))


def test_explain(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    res = mongo_connector.explain(datasource)
    assert list(res.keys()) == ['details', 'summary']


def test_unknown_database(mongo_connector, mongo_datasource):
    with pytest.raises(UnkwownMongoDatabase) as exc_info:
        datasource = mongo_datasource(database='unknown', collection='test_col', query={})
        mongo_connector.get_df(datasource)
    assert str(exc_info.value) == "Database 'unknown' doesn't exist"


def test_unknown_collection(mongo_connector, mongo_datasource):
    with pytest.raises(UnkwownMongoCollection) as exc_info:
        datasource = mongo_datasource(collection='unknown', query={})
        mongo_connector.get_df(datasource)
    assert str(exc_info.value) == "Collection 'unknown' doesn't exist"


def test_normalize_query():
    query = [{'$sort': [{'country': 1}, {'city': 1}]}]
    assert normalize_query(query, {}) == [{'$sort': SON([('country', 1), ('city', 1)])}]

    query = {'city': 'Test'}
    assert normalize_query(query, {}) == [{'$match': {'city': 'Test'}}]


def test_status_all_good(mongo_connector):
    assert mongo_connector.get_status() == {
        'status': True,
        'details': [
            ('Hostname resolved', True),
            ('Port opened', True),
            ('Host connection', True),
            ('Authenticated', True),
        ],
        'error': None,
    }


def test_status_bad_host(mongo_connector):
    mongo_connector.host = 'localhot'
    status = mongo_connector.get_status()
    assert status['status'] is False
    assert status['details'] == [
        ('Hostname resolved', False),
        ('Port opened', None),
        ('Host connection', None),
        ('Authenticated', None),
    ]
    assert 'not known' in status['error']


def test_status_bad_port(mongo_connector):
    mongo_connector.port += 1
    status = mongo_connector.get_status()
    assert status['status'] is False
    assert status['details'] == [
        ('Hostname resolved', True),
        ('Port opened', False),
        ('Host connection', None),
        ('Authenticated', None),
    ]
    assert 'Connection refused' in status['error']


def test_status_bad_port2(mongo_connector):
    mongo_connector.port = 123000
    assert mongo_connector.get_status() == {
        'status': False,
        'details': [
            ('Hostname resolved', True),
            ('Port opened', False),
            ('Host connection', None),
            ('Authenticated', None),
        ],
        'error': 'getsockaddrarg: port must be 0-65535.',
    }


def test_status_unreachable(mongo_connector, mocker):
    mocker.patch(
        'pymongo.MongoClient.server_info',
        side_effect=pymongo.errors.ServerSelectionTimeoutError('qwe'),
    )
    assert mongo_connector.get_status() == {
        'status': False,
        'details': [
            ('Hostname resolved', True),
            ('Port opened', True),
            ('Host connection', False),
            ('Authenticated', None),
        ],
        'error': 'qwe',
    }


def test_status_bad_username(mongo_connector):
    mongo_connector.username = 'bibou'
    assert mongo_connector.get_status() == {
        'status': False,
        'details': [
            ('Hostname resolved', True),
            ('Port opened', True),
            ('Host connection', True),
            ('Authenticated', False),
        ],
        'error': 'Authentication failed.',
    }


def test_get_form_empty_query(mongo_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = MongoDataSource.get_form(mongo_connector, current_config)
    assert form['required'] == ['domain', 'name', 'database', 'collection']
    assert form['properties']['database'] == {
        'title': 'Database',
        'type': 'string',
        'enum': ['admin', 'config', 'local', 'toucan'],
    }
    assert form['properties']['collection'] == {'title': 'Collection', 'type': 'string'}


def test_get_form_query_with_bad_database(mongo_connector):
    """It should raise an error"""
    current_config = {'database': 'qweqwe'}
    with pytest.raises(UnkwownMongoDatabase):
        MongoDataSource.get_form(mongo_connector, current_config)


def test_get_form_query_with_good_database(mongo_connector):
    """It should give suggestions of the collections"""
    current_config = {'database': 'toucan'}
    form = MongoDataSource.get_form(mongo_connector, current_config)
    assert form['required'] == ['domain', 'name', 'database', 'collection']
    assert form['properties']['database'] == {
        'title': 'Database',
        'type': 'string',
        'enum': ['admin', 'config', 'local', 'toucan'],
    }
    assert form['properties']['collection'] == {
        'title': 'Collection',
        'type': 'string',
        'enum': ['test_col'],
    }


def test_get_multiple_dfs(mocker, mongo_connector, mongo_datasource):
    """
    It should keep a client open and use the cache as much as possible when retrieving
    multiple dataframes
    """
    mongo_client_close = mocker.spy(pymongo.MongoClient, 'close')
    mongo_client = mocker.spy(pymongo, 'MongoClient')
    aggregate = mocker.spy(pymongo.collection.Collection, 'aggregate')
    validate_database = mocker.patch('toucan_connectors.mongo.mongo_connector.validate_database')

    queries = [
        {'domain': 'domain1'},
        {'domain': 'domain1', 'country': 'France'},
        {'domain': 'domain1', 'country': 'England'},
        {'country': 'England', 'domain': 'domain1'},
    ]
    with mongo_connector as con:
        for query in queries:
            datasource = mongo_datasource(collection='test_col', query=query)
            con.get_df(datasource)
    mongo_client.assert_called_once()
    assert aggregate.call_count == 4
    validate_database.assert_called_once()
    mongo_client_close.assert_called_once()


def test_validate_cache(mongo_connector):
    """It should cache the validation of a database for the same instance only"""
    con1 = mongo_connector
    assert con1.validate_database('toucan') is None
    con1.client.drop_database('toucan')
    assert con1.validate_database('toucan') is None, 'the cache should validate the dropped db'

    # A new connector should have a fresh cache
    con2 = con1.copy(deep=True)
    with pytest.raises(UnkwownMongoDatabase):
        con2.validate_database('toucan')
