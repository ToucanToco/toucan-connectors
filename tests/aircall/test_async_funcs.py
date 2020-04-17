"""Module containing tests with fake server"""
# from aiohttp import web
import pytest

import tests.general_helpers as helpers
from toucan_connectors.aircall.aircall_connector import fetch_page

fetch_fn_name = 'toucan_connectors.aircall.aircall_connector.fetch'

# we want this module's code to be checked against Python3.8
# versions prior to Python3.8 don't handle mocks same way for async functions
PY_VERSION_TO_CHECK = (3, 8)

is_py_version_older = helpers.check_py_version(PY_VERSION_TO_CHECK)


async def test_fetch_page_with_no_next(mocker):
    """Bearer version: tests that no next page returns an array of one response"""
    dataset = 'tags'
    fake_data = {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': None, 'current_page': 1}}
    if is_py_version_older:
        fake_data = helpers.build_future(fake_data)
    fake_fetch = mocker.patch(fetch_fn_name, return_value=fake_data)
    result = await fetch_page(dataset, [], {}, 10, 0)
    assert len(result) == 1
    first_dict = result[0]
    assert first_dict.get('data') is not None
    assert first_dict.get('meta') is not None

    if is_py_version_older:
        fake_fetch.assert_called_once_with(
            'https://proxy.bearer.sh/aircall_oauth/tags?per_page=50&page=1', {}
        )
    else:
        fake_fetch.assert_awaited_once_with(
            'https://proxy.bearer.sh/aircall_oauth/tags?per_page=50&page=1', {}
        )


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
    if is_py_version_older:
        data_list = [helpers.build_future(item) for item in data_list]
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=data_list)

    # limit is 10 and run is 0 i.e. this is the first run
    # want to see that, despite the limit being set to 10,
    # only two objects are sent in the array because no next_page_link
    # on second object means that there is no more to be fetched
    fake_res = await fetch_page(dataset, [], {}, 10, 0)
    assert len(fake_res) == 2
    if is_py_version_older:
        assert fake_fetch.call_count == 2
        fake_fetch.assert_called_with(
            'https://proxy.bearer.sh/aircall_oauth/calls?per_page=50&page=2', {}
        )
    else:
        assert fake_fetch.await_count == 2
        # last call made
        fake_fetch.assert_awaited_with(
            'https://proxy.bearer.sh/aircall_oauth/calls?per_page=50&page=2', {}
        )


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
    if is_py_version_older:
        data_list = [helpers.build_future(item) for item in data_list]
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=data_list)
    # limit is only 1 and run is 0 i.e. this is the first run
    res = await fetch_page(dataset, [], {}, 1, 0)
    assert len(res) == 1
    if is_py_version_older:
        fake_fetch.assert_called_once_with(
            'https://proxy.bearer.sh/aircall_oauth/users?per_page=50&page=1', {}
        )
    else:
        fake_fetch.assert_awaited_once_with(
            'https://proxy.bearer.sh/aircall_oauth/users?per_page=50&page=1', {}
        )


async def test_fetch_page_with_no_meta(mocker):
    """Tests that no meta object in response is not an issue"""
    dataset = 'calls'
    fake_data = {'data': {'stuff': 'stuff'}}
    if is_py_version_older:
        fake_data = helpers.build_future(fake_data)
    fake_fetch = mocker.patch(fetch_fn_name, return_value=fake_data)
    # despite there being a limit of 10 runs, only one run should occur
    res = await fetch_page(dataset, [], {}, 10, 0)
    assert len(res) == 1
    if is_py_version_older:
        fake_fetch.call_count == 1
    else:
        fake_fetch.assert_awaited_once()


async def test_fetch_page_with_error(mocker):
    """
    Tests error sent from API does not throw error on our server
    """
    dataset = 'tags'
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=Exception('oops'))
    with pytest.raises(Exception):
        await fetch_page(dataset, [], {}, 1, 0)
        fake_fetch.assert_awaited_once()


# async def send_no_link(req: web.Request) -> dict:
#     """Sends a response with no next_page_link in meta dict"""
#     fake_response = {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': None}}
#     return web.json_response(fake_response)


# async def send_no_meta(req: web.Request) -> dict:
#     """Sends a response with no meta dict"""
#     fake_response = {'data': {'stuff': 'stuff'}}
#     return web.json_response(fake_response)


# async def send_error(req: web.Request) -> dict:
#     """Sends a response with an error"""
#     fake_response = {'message': 'oops'}
#     return web.json_response(fake_response, status=400)


# async def send_multiple_links(req: web.Request) -> dict:
#     """Sends a response with multiple pages"""
#     fake_response = {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': '/foo?page=2'}}

#     if req.query.get('page') == '2':
#         fake_response = {'data': {'stuff': 'stuff 2'}, 'meta': {'next_page_link': None}}

#     return web.json_response(fake_response)


# async def test_fetch_page_with_no_next_page(aiohttp_client, loop):
#     """Tests fetch_page to see if it stops when no next_page_link"""
#     app = web.Application(loop=loop)
#     app.router.add_get('/foo', send_no_link)
#     client = await aiohttp_client(app)
#     # limit is 10 and run is 0 i.e. this is the first run and it should
#     # cap out at 10 runs max
#     res = await fetch_page('/foo', [], client, 10, 0)
#     assert len(res) == 1
#     res_dict = res[0]
#     assert res_dict.get('data') is not None
#     assert res_dict.get('meta') is not None


# async def test_fetch_page_with_next_page(aiohttp_client, loop, mocker):
#     """Test fetch_page to see multiple pages"""
#     app = web.Application(loop=loop)
#     endpoint = '/foo'
#     app.router.add_get(endpoint, send_multiple_links)
#     client = await aiohttp_client(app)
#     # limit is 10 and run is 0 i.e. this is the first run
#     res = await fetch_page(endpoint, [], client, 10, 0)
#     assert len(res) == 2


# async def test_fetch_page_with_low_limit(aiohttp_client, loop):
#     """Test fetch_page to see limit and current pass stopping recursion"""
#     app = web.Application(loop=loop)
#     endpoint = '/foo'
#     app.router.add_get(endpoint, send_multiple_links)
#     client = await aiohttp_client(app)
#     # limit is only 1 and run is 0 i.e. this is the first run
#     res = await fetch_page(endpoint, [], client, 1, 0)
#     assert len(res) == 1


# async def test_fetch_page_with_no_meta(aiohttp_client, loop):
#     """Tests that no meta object in response is not an issue"""
#     app = web.Application(loop=loop)
#     endpoint = '/bar'
#     app.router.add_get(endpoint, send_no_meta)
#     client = await aiohttp_client(app)
#     res = await fetch_page(endpoint, [], client, 10, 0)
#     assert len(res) == 1


# async def test_fetch_page_with_error(aiohttp_client, loop):
#     """
#     Tests error sent from API does not throw error on our server
#     """
#     app = web.Application(loop=loop)
#     endpoint = '/oops'
#     app.router.add_get(endpoint, send_error)
#     client = await aiohttp_client(app)
#     await fetch_page(endpoint, [], client, 1, 0)
