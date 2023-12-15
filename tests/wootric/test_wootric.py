import pytest
import responses
from aioresponses import aioresponses

import toucan_connectors.wootric.wootric_connector as woot


@pytest.fixture
def empty_token_cache():
    woot._TOKEN_CACHE = None


def test_wootric_url():
    assert woot.wootric_url('foo') == 'https://api.wootric.com/foo'
    assert woot.wootric_url('/foo') == 'https://api.wootric.com/foo'
    assert woot.wootric_url('v1/foo') == 'https://api.wootric.com/v1/foo'
    assert woot.wootric_url('/v1/foo') == 'https://api.wootric.com/v1/foo'


def test_fetch_data_stop_before_end():
    with aioresponses() as aiomock:
        base_query = 'https://api.wootric.com/v1/responses?access_token=x'
        for i in range(8):
            aiomock.get(f'{base_query}&page={i}&per_page=10', status=200, payload=[{'page': i}])
        data = woot.fetch_wootric_data(base_query, max_pages=6, batch_size=10)
        assert data == [
            {'page': 1},
            {'page': 2},
            {'page': 3},
            {'page': 4},
            {'page': 5},
            {'page': 6},
        ]


def test_fetch_data_stop_when_no_data():
    with aioresponses() as aiomock:
        base_query = 'https://api.wootric.com/v1/responses?access_token=x'
        for i in range(8):
            aiomock.get(f'{base_query}&page={i}&per_page=10', status=200, payload=[{'page': i}])
        for i in range(8, 11):
            aiomock.get(f'{base_query}&page={i}&per_page=10', status=200, payload=[])
        data = woot.fetch_wootric_data(base_query, batch_size=10, max_pages=13)
        assert data == [
            {'page': 1},
            {'page': 2},
            {'page': 3},
            {'page': 4},
            {'page': 5},
            {'page': 6},
            {'page': 7},
        ]


def test_fetch_data_custom_batch_size(mocker):
    max_pages = 3
    with aioresponses() as aiomock:
        base_query = 'https://api.wootric.com/v1/responses?access_token=x'
        for i in range(8):
            aiomock.get(f'{base_query}&page={i}&per_page=10', status=200, payload=[{'page': i}])
        for i in range(8, 11):
            aiomock.get(f'{base_query}&page={i}&per_page=10', status=200, payload=[])
        mocker.spy(woot, 'batch_fetch')
        data = woot.fetch_wootric_data(base_query, max_pages=max_pages, batch_size=10)
        assert data == [
            {'page': 1},
            {'page': 2},
            {'page': 3},
        ]
        assert woot.batch_fetch.call_count == 1


def test_fetch_data_filter_props():
    with aioresponses() as aiomock:
        base_query = 'https://api.wootric.com/v1/responses?access_token=x'
        for i in range(8):
            aiomock.get(
                f'{base_query}&page={i}&per_page=10',
                status=200,
                payload=[{'page': i, 'x': 1, 'y': 2}],
            )
        data = woot.fetch_wootric_data(base_query, props_fetched=('page', 'y'), max_pages=6, batch_size=10)
        assert data == [
            {'page': 1, 'y': 2},
            {'page': 2, 'y': 2},
            {'page': 3, 'y': 2},
            {'page': 4, 'y': 2},
            {'page': 5, 'y': 2},
            {'page': 6, 'y': 2},
        ]


@responses.activate
def test_wootric_get_df(empty_token_cache):
    datasource = woot.WootricDataSource(
        name='test',
        domain='test',
        query='responses',
        properties=['page', 'y'],
        batch_size=10,
        max_pages=3,
    )
    connector = woot.WootricConnector(name='wootric', type='wootric', client_id='cid', client_secret='cs')
    responses.add(
        responses.POST,
        'https://api.wootric.com/oauth/token',
        json={'access_token': 'x', 'expires_in': 10},
    )
    with aioresponses() as aiomock:
        base_query = 'https://api.wootric.com/v1/responses?access_token=x'
        for i in range(8):
            aiomock.get(
                f'{base_query}&page={i}&per_page=10',
                status=200,
                payload=[{'page': i, 'x': 1, 'y': 2}],
            )
        for i in range(8, 11):
            aiomock.get(f'{base_query}&page={i}&per_page=10', status=200, payload=[])
        df = connector.get_df(datasource)
    assert df.shape == (3, 2)
    assert set(df.columns) == {'page', 'y'}
    assert df[['page', 'y']].values.tolist() == [
        [1, 2],
        [2, 2],
        [3, 2],
    ]


@responses.activate
def test_token_cache_hit(mocker, empty_token_cache):
    connector = woot.WootricConnector(name='wootric', type='wootric', client_id='cid', client_secret='cs')
    responses.add(
        responses.POST,
        'https://api.wootric.com/oauth/token',
        json={'access_token': 'x', 'expires_in': 10},
    )
    mocker.spy(woot.WootricConnector, 'fetch_access_token')
    assert woot.access_token(connector) == 'x'
    assert woot.access_token(connector) == 'x'
    # fetch_access_token should have been called only once despite `access_token()`
    # was called twice
    assert connector.fetch_access_token.call_count == 1


@responses.activate
def test_token_cache_miss(mocker, empty_token_cache):
    connector = woot.WootricConnector(name='wootric', type='wootric', client_id='cid', client_secret='cs')
    # HACK: use a negative expire
    responses.add(
        responses.POST,
        'https://api.wootric.com/oauth/token',
        json={'access_token': 'x', 'expires_in': -10},
    )
    mocker.spy(woot.WootricConnector, 'fetch_access_token')
    assert woot.access_token(connector) == 'x'
    assert woot.access_token(connector) == 'x'
    # fetch_access_token should have been called twice since the token is marked
    # as expired
    assert connector.fetch_access_token.call_count == 2
