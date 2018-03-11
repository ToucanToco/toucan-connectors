import json
import os

import pandas as pd
import pymongo
import pymongo.errors
import pytest

from toucan_connectors.abstract_connector import MissingQueryParameter
from toucan_connectors.mongo import MongoConnector


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
    return MongoConnector(host='localhost', username='ubuntu', password='ilovetoucan',
                          database='toucan', port=mongo_server['port'])


def test_uri():
    mongo_con = MongoConnector(host='localhost', username='mister', password='superpass',
                               database='mydb', port=1793)
    assert mongo_con.uri == f'mongodb://mister:superpass@localhost:1793'

    mongo_con = MongoConnector(host='localhost', database='mydb', port=1793)
    assert mongo_con.uri == f'mongodb://localhost:1793'


def test_query(mongo_connector):
    # string query
    cur = mongo_connector.query(collection='test_col', query='domain1')
    docs = list(cur)
    assert len(docs) == 3
    assert {doc['country'] for doc in docs} == {'France', 'England', 'Germany'}

    # dict query (should be the same)
    cur = mongo_connector.query(collection='test_col', query={'domain': 'domain1'})
    assert list(cur) == docs

    # list query (should be the same)
    cur = mongo_connector.query(collection='test_col',
                                query=[{'$match': {'domain': 'domain1'}}])
    assert list(cur) == docs


def test_get_df(mongo_connector):
    with pytest.raises(MissingQueryParameter) as exc_info:
        mongo_connector.get_df(config={})
    assert str(exc_info.value) == '"collection" and "query" are mandatory to get a df'

    df = mongo_connector.get_df({'collection': 'test_col',
                                 'query': {'domain': 'domain1'}})
    expected = pd.DataFrame({'country': ['France', 'England', 'Germany'],
                             'language': ['French', 'English', 'German'],
                             'value': [20, 14, 17]})
    assert df.shape == (3, 5)
    assert df.columns.tolist() == ['_id', 'country', 'domain', 'language', 'value']
    assert df[['country', 'language', 'value']].equals(expected)
