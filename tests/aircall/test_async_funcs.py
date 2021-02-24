"""Module containing tests with fake server"""
from datetime import datetime

import pytest
from aiohttp import web

from tests.aircall.helpers import assert_called_with
from toucan_connectors.aircall.aircall_connector import (
    AircallDataset,
    AircallRateLimitExhaustedException,
    fetch,
    fetch_page,
)

fetch_fn_name = 'toucan_connectors.aircall.aircall_connector.fetch'

FAKE_DATA = {'foo': 'bar', 'baz': 'fudge'}


async def send_200_success(req: web.Request):
    """Send a response with a success."""
    return web.json_response(FAKE_DATA, status=200)


async def send_rate_limit(req: web.Request):
    """Send an error response with details in headers"""
    return web.json_response(
        text='ERROR',
        headers={
            'X-AircallApi-Limit': '60',
            'X-AircallApi-Remaining': '0',
            'X-AircallApi-Reset': '1614093780',
        },
    )


async def test_fetch(aiohttp_client, loop):
    """It should return a properly-formed dictionary."""
    app = web.Application(loop=loop)
    app.router.add_get('/foo', send_200_success)

    client = await aiohttp_client(app)
    res = await fetch('/foo', client)

    assert res == FAKE_DATA


async def test_fetch_limit(aiohttp_client, loop):
    """It should raise an AircallRateLimitExhaustedException"""
    app = web.Application(loop=loop)
    app.router.add_get('/foo', send_rate_limit)

    client = await aiohttp_client(app)
    with pytest.raises(AircallRateLimitExhaustedException):
        await fetch('/foo', client)


async def test_fetch_with_params(aiohttp_client, loop):
    """It should return a properly-formed dictionary."""
    app = web.Application(loop=loop)
    app.router.add_get('/foo', send_200_success)

    client = await aiohttp_client(app)
    res = await fetch('/foo', client, query_params={'foo': 'bar'})

    assert res == FAKE_DATA


async def test_fetch_page_with_no_next(mocker):
    """Bearer version: tests that no next page returns an array of one response"""
    dataset = 'tags'
    fake_data = {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': None, 'current_page': 1}}
    fake_data = fake_data
    fake_fetch = mocker.patch(fetch_fn_name, return_value=fake_data)
    result = await fetch_page(dataset, [], {}, 10, 0)
    assert len(result) == 1
    first_dict = result[0]
    assert 'data' in first_dict
    assert 'meta' in first_dict

    assert_called_with(fake_fetch, ['https://api.aircall.io/v1/tags?per_page=50&page=1', {}])


async def test_fetch_page_with_next_page(mocker):
    """Test fetch_page to see multiple pages"""
    dataset = 'calls'
    data_list = [
        {
            'data': {'stuff': 'stuff'},
            'meta': {'next_page_link': '/calls?page=2', 'current_page': 1},
        },
        {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': None, 'current_page': 2}},
    ]
    data_list = [item for item in data_list]
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=data_list)

    # limit is 10 and run is 0 i.e. this is the first run
    # want to see that, despite the limit being set to 10,
    # only two objects are sent in the array because no next_page_link
    # on second object means that there is no more to be fetched
    fake_res = await fetch_page(dataset, [], {}, 10, 0)
    assert len(fake_res) == 2

    assert_called_with(fake_fetch, ['https://api.aircall.io/v1/calls?per_page=50&page=2', {}], 2)


async def test_fetch_page_with_next_page_negative_limit(mocker):
    """Test fetch_page to see multiple pages with a negative limit, it's not supposed to happen
    as with have a validation on limit which must be > 0"""
    dataset = 'calls'
    data_list = [
        {
            'data': {'stuff': 'stuff'},
            'meta': {'next_page_link': '/calls?page=2', 'current_page': 1},
        },
        {
            'data': {'stuff': 'stuff'},
            'meta': {'next_page_link': '/calls?page=3', 'current_page': 2},
        },
        {'data': {'stuff': 'stuff'}},
    ]
    data_list = [item for item in data_list]
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=data_list)
    fake_res = await fetch_page(dataset, [], {}, -1, 0)
    assert len(fake_res) == 3

    assert_called_with(fake_fetch, ['https://api.aircall.io/v1/calls?per_page=50&page=3', {}], 3)


async def test_fetch_page_with_low_limit(mocker):
    """Test fetch_page to see limit and current pass stopping recursion"""
    dataset = 'users'
    data_list = [
        {
            'data': {'stuff': 'stuff'},
            'meta': {'next_page_link': '/users?page=2', 'current_page': 1},
        },
        {
            'data': {'stuff': 'stuff'},
            'meta': {'next_page_link': '/users?page=3', 'current_page': 2},
        },
        {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': None, 'current_page': 3}},
    ]
    data_list = [item for item in data_list]
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=data_list)
    # limit is only 1 and run is 0 i.e. this is the first run
    res = await fetch_page(dataset, [], {}, 1, 0)
    assert len(res) == 1

    assert_called_with(fake_fetch, ['https://api.aircall.io/v1/users?per_page=50&page=1', {}])


async def test_fetch_page_with_no_meta(mocker):
    """Tests that no meta object in response is not an issue"""
    dataset = 'calls'
    fake_data = {'data': {'stuff': 'stuff'}}
    fake_data = fake_data
    fake_fetch = mocker.patch(fetch_fn_name, return_value=fake_data)
    # despite there being a limit of 10 runs, only one run should occur
    res = await fetch_page(dataset, [], {}, 10, 0)
    assert len(res) == 1
    assert_called_with(fake_fetch, expected_count=1)


async def test_fetch_page_with_error(mocker):
    """
    Tests error sent from API goes through a retry policy and then throws an exception
    """
    dataset = 'tags'
    fake_data = {'error': 'Oops!', 'troubleshoot': 'Blah blah blah'}
    fake_data = fake_data
    fake_fetch = mocker.patch(fetch_fn_name, return_value=fake_data)
    with pytest.raises(Exception) as e:
        await fetch_page(dataset, [], {}, 1, 0)
    fake_fetch.call_count == 4
    assert str(e.value) == 'Aborting Aircall requests due to Oops!'


async def test_fetch_page_with_params(mocker):
    """
    Tests fetch page providing start & end dates
    """
    dataset = 'calls'
    fake_data = {'data': {'stuff': 'stuff'}}
    fake_fetch = mocker.patch(fetch_fn_name, return_value=fake_data)
    await fetch_page(dataset, [], {}, 1, 0, query_params={'from': 1609459200, 'to': 1612137599})
    assert fake_fetch.call_args_list[0][0][2] == {'from': 1609459200, 'to': 1612137599}


async def test_query_params_not_named_arg(mocker):
    """
    Check that fetch_page fails if query params are not given
    as named arg
    """
    ds = AircallDataset('calls')
    params = {'bla': 'bla'}

    with pytest.raises(TypeError):
        await fetch_page(ds, [], 'session', 0, 0, 0, 0, params)


async def test_fetch_page_rate_limited(mocker):
    """
    Check that fetch_page is able to pause when
    an AircallRateLimitExhaustedException exception is throw by fetch
    """
    ds = AircallDataset('calls')
    fake_data = {'data': {'stuff': 'stuff'}}
    mocker.patch(
        fetch_fn_name, side_effect=[AircallRateLimitExhaustedException('1610000003'), fake_data]
    )
    mockeddatetime = mocker.patch('toucan_connectors.aircall.aircall_connector.datetime')
    mocked_utcnow = mockeddatetime.utcnow
    mocked_utcnow.return_value = datetime(2021, 1, 7, 6, 13, 20)
    mocked_timestamp = mockeddatetime.timestamp
    mocked_timestamp.return_value = 1610000001
    mockedsleep = mocker.patch('toucan_connectors.aircall.aircall_connector.time.sleep')
    await fetch_page(ds, [], 'session', 0, 0, 0, 0)
    mockedsleep.assert_called_once()
    assert mockedsleep.call_args_list[0][0][0] == 1
