import json
import os

import pandas as pd
import pymongo
import pymongo.errors
import pytest

from toucan_connectors.mongo.mongo_connector import MongoDataSource, MongoConnector


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


@pytest.fixture()
def mongo_connector(mongo_server):
    return MongoConnector(name='mycon', host='localhost', database='toucan',
                          port=mongo_server['port'], username='ubuntu', password='ilovetoucan')


@pytest.fixture()
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


def test_get_df(mongo_connector, mongo_datasource):
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
