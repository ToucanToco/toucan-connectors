import os
import re
from datetime import datetime
from typing import TYPE_CHECKING, Callable

import pandas as pd
import pymongo
import pymongo.errors
import pytest
from bson.son import SON

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.mongo.mongo_connector import (
    MongoConnector,
    MongoDataSource,
    UnkwownMongoCollection,
    UnkwownMongoDatabase,
    _format_explain_result,
    normalize_query,
)
from toucan_connectors.toucan_connector import MalformedVersion, UnavailableVersion

if TYPE_CHECKING:
    from typing import Self


@pytest.fixture(scope='module')
def mongo_server(service_container):
    def check_and_feed(host_port):
        client = pymongo.MongoClient(f'mongodb://ubuntu:ilovetoucan@localhost:{host_port}')

        docs_path = f'{os.path.dirname(__file__)}/fixtures/docs.json'
        with open(docs_path) as f:
            docs_json = f.read()
        docs = JsonWrapper.loads(docs_json)
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


def test_username_password():
    # password not set
    mongo_connector = MongoConnector(name='mycon', host='localhost', port=22)
    assert mongo_connector._get_mongo_client_kwargs() == {
        'host': 'localhost',
        'port': 22,
        'maxPoolSize': 1,
    }

    # password set to None
    mongo_connector = MongoConnector(name='mycon', host='localhost', port=22, password=None)
    assert mongo_connector._get_mongo_client_kwargs() == {
        'host': 'localhost',
        'port': 22,
        'maxPoolSize': 1,
    }

    # password set without username
    with pytest.raises(ValueError, match='username must be set'):
        MongoConnector(name='mycon', host='localhost', port=22, password='bibou')

    # password and user set
    mongo_connector = MongoConnector(
        name='mycon', host='localhost', port=22, username='pika', password='bibou'
    )
    assert mongo_connector._get_mongo_client_kwargs() == {
        'host': 'localhost',
        'port': 22,
        'username': 'pika',
        'password': 'bibou',
        'maxPoolSize': 1,
    }


def test_client_with_detailed_params():
    connector = MongoConnector(name='my_mongo_con', host='myhost', port='123')
    with connector.client() as client:
        assert isinstance(client, pymongo.MongoClient)


def test_client_with_mongo_uri():
    connector = MongoConnector(name='my_mongo_con', host='mongodb://myuser:mypassword@myhost:123')
    with connector.client() as client:
        assert isinstance(client, pymongo.MongoClient)


def test_client_args_with_mongo_uri(mocker):
    """It should not pass any other parameter than the host to MongoClient"""
    mongo_client_mock = mocker.patch('toucan_connectors.mongo.mongo_connector.pymongo.MongoClient')
    connector = MongoConnector(name='my_mongo_con', host='mongodb://myuser:mypassword@myhost:123')
    with connector.client():
        mongo_client_mock.assert_called_with(
            host='mongodb://myuser:mypassword@myhost:123', maxPoolSize=1
        )


def test_client_args_with_ssl(mocker):
    """It should forward parameters to mongo client"""
    mongo_client_mock = mocker.patch('toucan_connectors.mongo.mongo_connector.pymongo.MongoClient')
    connector = MongoConnector(
        name='my_mongo_con', host='myhost', password='blah', username='jean', ssl=True
    )
    with connector.client():
        mongo_client_mock.assert_called_with(
            host='myhost', ssl=True, password='blah', username='jean', maxPoolSize=1
        )


def test_get_df_no_query(
    mongo_connector: MongoConnector, mongo_datasource: Callable[..., MongoDataSource]
):
    """It should return the whole collection by default"""
    ds = mongo_datasource(collection='test_col')
    df = mongo_connector.get_df(ds)
    assert df.shape == (5, 6)


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

        def __enter__(self) -> 'Self':
            return self

        def __exit__(self, *_) -> None:
            pass

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

    snock.assert_called_with(
        host='localhost', username='ubuntu', password='ilovetoucan', port=22, maxPoolSize=1
    )
    assert snock.call_count == 2  # Only one client should have been instanciated per call

    aggregate.assert_called_with([{'$match': {'domain': 'domain1'}}])
    assert aggregate.call_count == 2


def test_get_df_live(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df(datasource)
    expected = pd.DataFrame(
        {
            'country': ['France', 'France', 'England', 'Germany', 'USA'],
            'language': ['French', 'French', 'English', 'German', 'English'],
            'value': [20, 20, 14, 17, 28],
            'name': ['François', 'Marie', 'Evelyn', 'Hans', 'Scarlett'],
        }
    )
    assert df.shape == (5, 6)
    assert set(df.columns) == {'_id', 'country', 'domain', 'language', 'value', 'name'}
    assert df[['country', 'language', 'value', 'name']].equals(expected)

    datasource = mongo_datasource(
        collection='test_col', query=[{'$match': {'domain': 'domain1'}}, {'$sort': [{'pays': 1}]}]
    )
    df2 = mongo_connector.get_df(datasource)
    assert df2.equals(df)


def test_get_df_with_permissions(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df(
        datasource, permissions={'column': 'country', 'operator': 'eq', 'value': 'France'}
    )
    expected = pd.DataFrame(
        {
            'country': ['France', 'France'],
            'language': ['French', 'French'],
            'value': [20, 20],
            'name': ['François', 'Marie'],
        }
    )
    assert datasource.query == [
        {'$match': {'$and': [{'domain': 'domain1'}, {'country': {'$eq': 'France'}}]}}
    ]
    assert df.shape == (2, 6)
    assert set(df.columns) == {'_id', 'country', 'domain', 'language', 'value', 'name'}
    assert df[['country', 'language', 'value', 'name']].equals(expected)

    datasource = mongo_datasource(collection='test_col', query=[{'$match': {'domain': 'domain1'}}])
    df = mongo_connector.get_df(
        datasource, permissions={'column': 'country', 'operator': 'eq', 'value': 'France'}
    )
    expected = pd.DataFrame(
        {
            'country': ['France', 'France'],
            'language': ['French', 'French'],
            'value': [20, 20],
            'name': ['François', 'Marie'],
        }
    )
    assert datasource.query == [
        {'$match': {'$and': [{'domain': 'domain1'}, {'country': {'$eq': 'France'}}]}}
    ]
    assert df.shape == (2, 6)
    assert set(df.columns) == {'_id', 'country', 'domain', 'language', 'value', 'name'}
    assert df[['country', 'language', 'value', 'name']].equals(expected)


def test_get_slice(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    res = mongo_connector.get_slice(datasource)
    assert res.stats.total_returned_rows == 5
    assert res.stats.total_rows == 5
    assert res.df.shape == (5, 5)
    assert res.df['country'].tolist() == ['France', 'France', 'England', 'Germany', 'USA']

    # With a limit
    res = mongo_connector.get_slice(datasource, limit=1)
    expected = pd.DataFrame({'country': ['France'], 'language': ['French'], 'value': [20]})
    assert res.stats.total_returned_rows == 5
    assert res.stats.total_rows == 5
    assert res.df.shape == (1, 5)
    assert res.df[['country', 'language', 'value']].equals(expected)

    # With a offset
    res = mongo_connector.get_slice(datasource, offset=1)
    assert res.stats.total_returned_rows == 5
    assert res.stats.total_rows == 5
    assert res.df.shape == (4, 5)
    assert res.df['country'].tolist() == ['France', 'England', 'Germany', 'USA']

    # With both
    res = mongo_connector.get_slice(datasource, offset=1, limit=1)
    assert res.stats.total_returned_rows == 5
    assert res.stats.total_rows == 5
    assert res.df.shape == (1, 5)
    assert res.df.loc[0, 'country'] == 'France'


def test_get_slice_with_group_agg(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(
        collection='test_col',
        query=[
            {'$match': {'domain': 'domain1'}},
            {'$group': {'_id': {'country': '$country'}}},
            {'$project': {'pays': '$_id.country', '_id': 0}},
            {'$sort': [{'pays': 1}]},
        ],
    )
    dataslice = mongo_connector.get_slice(datasource, limit=1)
    assert dataslice.stats.total_returned_rows == 4
    assert dataslice.stats.total_rows == 4
    assert dataslice.df.shape == (1, 1)
    assert dataslice.df.iloc[0].pays in ['France', 'England', 'Germany']


def test_get_slice_no_limit(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    ds = mongo_connector.get_slice(datasource, limit=None)
    assert ds.stats.total_returned_rows == 5
    expected = pd.DataFrame(
        {
            'country': ['France', 'France', 'England', 'Germany', 'USA'],
            'language': ['French', 'French', 'English', 'German', 'English'],
            'value': [20, 20, 14, 17, 28],
            'name': ['François', 'Marie', 'Evelyn', 'Hans', 'Scarlett'],
        }
    )
    assert ds.df.shape == (5, 5)
    assert ds.df[['country', 'language', 'value', 'name']].equals(expected)


def test_get_slice_empty(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'unknown'})
    dataslice = mongo_connector.get_slice(datasource, limit=1)
    assert dataslice.stats.total_returned_rows == 0
    assert dataslice.df.shape == (0, 0)


def test_get_slice_max_count(mongo_connector, mongo_datasource, mocker):
    """
    It should limit mongo's count operation to 1M rows

    We're not going to insert a million rows in mongo just for this test,
    so we mock the execution of the query.
    """
    aggregate = mocker.spy(pymongo.collection.Collection, 'aggregate')

    datasource = mongo_datasource(collection='test_col', query={'domain': 'unknown'})
    mongo_connector.get_slice(datasource, limit=50)

    aggregate.assert_called_once()
    # count facet must be limited
    assert '$limit' in aggregate.call_args[0][1][1]['$facet']['count'][0]
    assert aggregate.call_args[0][1][1]['$facet']['count'][0]['$limit'] > 0


def test_get_slice_with_regex(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    regex = re.compile('g')
    slice = mongo_connector.get_slice_with_regex(
        datasource,
        {
            'or': [
                {'country': regex},
                {'language': regex},
            ]
        },
    )
    pd.testing.assert_series_equal(
        slice.df['country'], pd.Series(['England', 'Germany', 'USA'], name='country')
    )


def test_get_df_with_regex(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df_with_regex(datasource, {'and': [{'country': re.compile('r.*a')}]})
    pd.testing.assert_series_equal(
        df['country'], pd.Series(['France', 'France', 'Germany'], name='country')
    )


def test_get_df_with_regex_with_projection_stage(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(
        collection='test_col',
        query=[{'$match': {'domain': 'domain1'}}, {'$addFields': {'new_country_col': '$country'}}],
    )
    df = mongo_connector.get_df_with_regex(
        datasource, {'and': [{'new_country_col': re.compile('r.*a')}]}
    )
    pd.testing.assert_series_equal(
        df['new_country_col'], pd.Series(['France', 'France', 'Germany'], name='new_country_col')
    )


def test_get_df_with_regex_with_integers(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(
        collection='test_col',
        query=[{'$match': {'domain': 'domain1'}}],
    )
    df = mongo_connector.get_df_with_regex(datasource, {'and': [{'value': re.compile('^20$')}]})
    assert df.to_dict(orient='records') == [
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'François',
        },
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'Marie',
        },
    ]


def test_get_df_with_regex_case_sensitiveness(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(
        collection='test_col',
        query=[{'$match': {'domain': 'domain1'}}],
    )
    df = mongo_connector.get_df_with_regex(
        datasource, {'and': [{'country': re.compile('^FrAn.*')}]}
    )
    assert df.to_dict(orient='records') == [
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'François',
        },
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'Marie',
        },
    ]


def test_get_df_with_regex_with_limit(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df_with_regex(
        datasource, {'and': [{'country': re.compile('r.*a')}]}, limit=1
    )
    pd.testing.assert_series_equal(df['country'], pd.Series(['France'], name='country'))


def test_get_df_with_regex_with_offset_and_limit(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    df = mongo_connector.get_df_with_regex(
        datasource, {'and': [{'country': re.compile('r.*a')}]}, limit=1, offset=2
    )
    pd.testing.assert_series_equal(df['country'], pd.Series(['Germany'], name='country'))


def test_get_df_with_regex_and(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(
        collection='test_col',
        query=[{'$match': {'domain': 'domain1'}}],
    )
    assert mongo_connector.get_df_with_regex(
        datasource, {'and': [{'country': re.compile('France'), 'name': re.compile('Marie')}]}
    ).to_dict(orient='records') == [
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'Marie',
        }
    ]


def test_get_df_with_regex_or(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(
        collection='test_col',
        query=[{'$match': {'domain': 'domain1'}}],
    )
    assert mongo_connector.get_df_with_regex(
        datasource, {'or': [{'country': re.compile('France'), 'name': re.compile('Marie')}]}
    ).to_dict(orient='records') == [
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'Marie',
        }
    ]
    assert mongo_connector.get_df_with_regex(
        datasource, {'or': [{'country': re.compile('France')}, {'name': re.compile('Marie')}]}
    ).to_dict(orient='records') == [
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'François',
        },
        {
            'domain': 'domain1',
            'country': 'France',
            'language': 'French',
            'value': 20,
            'name': 'Marie',
        },
    ]


def test_explain(mongo_connector: MongoConnector, mongo_datasource: Callable[..., MongoDataSource]):
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

    query = [
        {'$match': {'domain': 'fifa23_players'}},
        {'$project': {'Name': 1, 'Height': 1, 'Nationality': 1}},
        {'$match': {'Nationality': {}}},
        {'$sort': {'Height': -1}},
    ]
    expected = [
        {'$match': {'domain': 'fifa23_players'}},
        {'$project': {'Name': 1, 'Height': 1, 'Nationality': 1}},
        # Here, we want a match that passes everything, we don't want to remove the step, as a
        # [{'$match': {}}], would result in an empty list, which would be invalid.
        {'$match': {}},
        {'$sort': {'Height': -1}},
    ]

    assert normalize_query(query, {}) == expected
    query_with_nulls_in_match = [
        {'$match': {}},
        {
            '$match': {
                'one': {'$eq': None},
                'two': {'$eq': None},
                'three': {'$ne': None},
            }
        },
    ]
    assert normalize_query(query_with_nulls_in_match, {}) == query_with_nulls_in_match


def test_status_all_good(mongo_connector):
    assert mongo_connector.get_status() == ConnectorStatus(
        status=True,
        details=[
            ('Hostname resolved', True),
            ('Port opened', True),
            ('Host connection', True),
            ('Authenticated', True),
        ],
        error=None,
    )


def test_status_bad_host(mongo_connector):
    mongo_connector.host = 'localhot'
    mongo_connector.port = 42
    status = mongo_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Hostname resolved', False),
        ('Port opened', None),
        ('Host connection', None),
        ('Authenticated', None),
    ]
    assert len(status.error) > 0


def test_status_bad_port(mongo_connector):
    mongo_connector.port += 1
    status = mongo_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Hostname resolved', True),
        ('Port opened', False),
        ('Host connection', None),
        ('Authenticated', None),
    ]
    assert 'Connection refused' in status.error


def test_status_bad_port2(mongo_connector):
    mongo_connector.port = 123000
    status = mongo_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Hostname resolved', True),
        ('Port opened', False),
        ('Host connection', None),
        ('Authenticated', None),
    ]
    assert 'port must be 0-65535.' in status.error


def test_status_unreachable(mongo_connector, mocker):
    mocker.patch(
        'pymongo.MongoClient.server_info',
        side_effect=pymongo.errors.ServerSelectionTimeoutError('qwe'),
    )
    assert mongo_connector.get_status() == ConnectorStatus(
        status=False,
        details=[
            ('Hostname resolved', True),
            ('Port opened', True),
            ('Host connection', False),
            ('Authenticated', None),
        ],
        error='qwe',
    )


def test_get_engine_version(mocker, mongo_connector):
    # Should be a valide semver version converted to tuple
    mocker.patch('pymongo.MongoClient.server_info', return_value={'version': '3.4.5'})
    assert mongo_connector.get_engine_version() == (3, 4, 5)

    # Should raise a MalformedVersion error
    mocker.patch(
        'pymongo.MongoClient.server_info', return_value={'version': '--bad-version-format-'}
    )
    with pytest.raises(MalformedVersion):
        assert mongo_connector.get_engine_version()

    # Should raise an UnavailableVersion error
    mocker.patch('pymongo.MongoClient.server_info', return_value=None)
    with pytest.raises(UnavailableVersion):
        assert mongo_connector.get_engine_version()


def test_status_bad_username(mongo_connector):
    mongo_connector.username = 'bibou'
    status = mongo_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Hostname resolved', True),
        ('Port opened', True),
        ('Host connection', True),
        ('Authenticated', False),
    ]
    assert 'Authentication failed' in status.error


def test_get_form_empty_query(mongo_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = MongoDataSource.get_form(mongo_connector, current_config)
    assert form['required'] == ['domain', 'name', 'database', 'collection']
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['admin', 'config', 'local', 'toucan'],
    }
    assert form['properties']['collection'] == {
        'type': 'string',
        'title': 'Collection',
        'description': 'The name of the collection you want to query',
    }


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
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['admin', 'config', 'local', 'toucan'],
    }
    assert form['properties']['collection'] == {'$ref': '#/definitions/collection'}
    assert form['definitions']['collection'] == {
        'title': 'collection',
        'description': 'An enumeration.',
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
    assert aggregate.call_count == 4
    assert mongo_client.call_count == 4
    assert mongo_client_close.call_count == 4
    validate_database.assert_called_once()


def test_validate_cache(mongo_connector: MongoConnector):
    """It should cache the validation of a database for the same instance only"""
    con1 = mongo_connector
    with con1.client() as client:
        assert con1._validate_database(client, 'toucan') is None
        client.drop_database('toucan')
        assert (
            con1._validate_database(client, 'toucan') is None
        ), 'the cache should validate the dropped db'

    # A new connector should have a fresh cache
    con2 = con1.model_copy()
    with pytest.raises(UnkwownMongoDatabase):
        with con2.client() as client:
            con2._validate_database(client, 'toucan')


def test_format_no_explain_result():
    """It should return None on empty result"""
    assert _format_explain_result({}) is None
    assert _format_explain_result(None) is None


def test_format_no_stage_explain_result():
    """It should handle explain result without 'stages' entry"""
    explain_result = {
        'executionStats': {'foo': 'bar'},
        'ok': 1.0,
        'queryPlanner': {'winningPlan': {}},
        'serverInfo': {
            'gitVersion': '20364840b8f1af16917e4c23c1b5f5efd8b352f8',
            'host': '24a5e90be103',
            'port': 27017,
            'version': '4.2.6',
        },
    }
    assert _format_explain_result(explain_result) == {
        'details': {
            'executionStats': {'foo': 'bar'},
            'ok': 1.0,
            'queryPlanner': {'winningPlan': {}},
        },
        'summary': [{'foo': 'bar'}],
    }


def test_format_stages_explain_result():
    """It should handle explain results with 'stages' entries"""
    explain_result = {
        'stages': [
            {
                '$cursor': {
                    'query': {},
                    'queryPlanner': {
                        'plannerVersion': 1,
                        'namespace': 'database.collection',
                        'parsedQuery': {},
                        'winningPlan': {},
                    },
                    'executionStats': {
                        'executionSuccess': True,
                        'nReturned': 5815,
                        'executionTimeMillis': 5271,
                        'totalKeysExamined': 1432800,
                        'totalDocsExamined': 1432799,
                        'executionStages': {'stage': 'FETCH', 'inputStage': {}},
                    },
                },
            },
            {'$group': {}},
            {'$project': {}},
        ],
        'ok': 1,
    }
    assert _format_explain_result(explain_result) == {
        'details': {
            'ok': 1,
            'stages': [
                {
                    '$cursor': {
                        'executionStats': {
                            'executionStages': {'inputStage': {}, 'stage': 'FETCH'},
                            'executionSuccess': True,
                            'executionTimeMillis': 5271,
                            'nReturned': 5815,
                            'totalDocsExamined': 1432799,
                            'totalKeysExamined': 1432800,
                        },
                        'query': {},
                        'queryPlanner': {
                            'namespace': 'database.collection',
                            'parsedQuery': {},
                            'plannerVersion': 1,
                            'winningPlan': {},
                        },
                    },
                },
                {'$group': {}},
                {'$project': {}},
            ],
        },
        'summary': [
            {
                'executionStages': {'inputStage': {}, 'stage': 'FETCH'},
                'executionSuccess': True,
                'executionTimeMillis': 5271,
                'nReturned': 5815,
                'totalDocsExamined': 1432799,
                'totalKeysExamined': 1432800,
            },
        ],
    }


def test_get_cache_key(mongo_connector):
    """
    It should not raise 'TypeError: Object of type 'MongoClient' is not JSON serializable'
    """
    assert mongo_connector.client is not None
    assert isinstance(mongo_connector.get_cache_key(), str)

    conn1 = MongoConnector(name='aaa', host='here', port=42, username='me', password='s3cr3t')
    conn2 = MongoConnector(name='aaa', host='here', port=42, username='me', password='?')
    assert conn1.get_cache_key() != conn2.get_cache_key()


def test_get_cache_key_with_datasource(mongo_connector, mongo_datasource):
    datasource = mongo_datasource(collection='test_col', query={'domain': 'domain1'})
    datasource_with_parameters = mongo_datasource(
        collection='test_col', query={'domain': '{{ DOMAIN }}'}, parameters={'DOMAIN': 'domain1'}
    )
    assert mongo_connector.get_cache_key(datasource) == mongo_connector.get_cache_key(
        datasource_with_parameters
    )

    datasource_with_bad_parameters = mongo_datasource(
        collection='test_col', query={'domain': '{{ DOMAIN }}'}, parameters={'DOMAIN': 'domain3'}
    )
    assert mongo_connector.get_cache_key(datasource) != mongo_connector.get_cache_key(
        datasource_with_bad_parameters
    )

    datasource_with_extra_parameters = mongo_datasource(
        collection='test_col',
        query={'domain': '{{ DOMAIN }}'},
        parameters={'DOMAIN': 'domain1', 'FOO': 'BAR'},
    )
    assert mongo_connector.get_cache_key(datasource) == mongo_connector.get_cache_key(
        datasource_with_extra_parameters
    )


def test_get_cache_key_with_dates(mongo_connector, mongo_datasource):
    """
    Mongo queries can contain objects, like dates, that are not JSON serializable.
    The cache key must be away of the type of these objects
    """
    datasource_with_date = mongo_datasource(
        collection='test_col',
        query={'domain': 'plop', 'date': datetime(2021, 10, 4)},
        parameters={'DOMAIN': 'domain1', 'FOO': 'BAR'},
    )
    # should not fail
    cache_key_date = mongo_connector.get_cache_key(datasource_with_date)

    datasource_with_str = mongo_datasource(
        collection='test_col',
        query={'domain': 'plop', 'date': str(datetime(2021, 10, 4))},
        parameters={'DOMAIN': 'domain1', 'FOO': 'BAR'},
    )
    cache_key_str = mongo_connector.get_cache_key(datasource_with_str)

    assert cache_key_str != cache_key_date
