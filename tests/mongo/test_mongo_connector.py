import json
import os

import pymongo
import pymongo.errors
import pytest

from connectors.abstract_connector import MissingQueryParameter
from connectors.mongo import MongoConnector


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
def connector(mongo_server):
    return MongoConnector(host='localhost', username='ubuntu', password='ilovetoucan',
                          database='toucan', port=mongo_server['port'])


def test_missing_collection_param(connector):
    with connector as mongo_connector:
        with pytest.raises(MissingQueryParameter) as exc_info:
            mongo_connector.get_df(config={})
        assert str(exc_info.value) == '"collection" and "query" are mandatory to get a df'


def test_query(connector):
    with connector as mongo_connector:
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
