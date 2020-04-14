"""Module containing tests with fake server"""
from aiohttp import web

from toucan_connectors.aircall.aircall_connector import fetch_page


async def send_no_link(req: web.Request) -> dict:
    """Sends a response with no next_page_link in meta dict"""
    fake_response = {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': None}}
    return web.json_response(fake_response)


async def send_no_meta(req: web.Request) -> dict:
    """Sends a response with no meta dict"""
    fake_response = {'data': {'stuff': 'stuff'}}
    return web.json_response(fake_response)


async def send_error(req: web.Request) -> dict:
    """Sends a response with an error"""
    fake_response = {'message': 'oops'}
    return web.json_response(fake_response, status=400)


async def send_multiple_links(req: web.Request) -> dict:
    """Sends a response with multiple pages"""
    fake_response = {'data': {'stuff': 'stuff'}, 'meta': {'next_page_link': '/foo?page=2'}}

    if req.query.get('page') == '2':
        fake_response = {'data': {'stuff': 'stuff 2'}, 'meta': {'next_page_link': None}}

    return web.json_response(fake_response)


async def test_fetch_page_with_no_next_page(aiohttp_client, loop):
    """Tests fetch_page to see if it stops when no next_page_link"""
    app = web.Application(loop=loop)
    app.router.add_get('/foo', send_no_link)
    client = await aiohttp_client(app)
    # limit is 10 and run is 0 i.e. this is the first run and it should
    # cap out at 10 runs max
    res = await fetch_page('/foo', [], client, 10, 0)
    assert len(res) == 1
    res_dict = res[0]
    assert res_dict.get('data') is not None
    assert res_dict.get('meta') is not None


async def test_fetch_page_with_next_page(aiohttp_client, loop, mocker):
    """Test fetch_page to see multiple pages"""
    app = web.Application(loop=loop)
    endpoint = '/foo'
    app.router.add_get(endpoint, send_multiple_links)
    client = await aiohttp_client(app)
    # limit is 10 and run is 0 i.e. this is the first run
    res = await fetch_page(endpoint, [], client, 10, 0)
    assert len(res) == 2


async def test_fetch_page_with_low_limit(aiohttp_client, loop):
    """Test fetch_page to see limit and current pass stopping recursion"""
    app = web.Application(loop=loop)
    endpoint = '/foo'
    app.router.add_get(endpoint, send_multiple_links)
    client = await aiohttp_client(app)
    # limit is only 1 and run is 0 i.e. this is the first run
    res = await fetch_page(endpoint, [], client, 1, 0)
    assert len(res) == 1


async def test_fetch_page_with_no_meta(aiohttp_client, loop):
    """Tests that no meta object in response is not an issue"""
    app = web.Application(loop=loop)
    endpoint = '/bar'
    app.router.add_get(endpoint, send_no_meta)
    client = await aiohttp_client(app)
    res = await fetch_page(endpoint, [], client, 10, 0)
    assert len(res) == 1


async def test_fetch_page_with_error(aiohttp_client, loop):
    """
    Tests error sent from API does not throw error on our server
    """
    app = web.Application(loop=loop)
    endpoint = '/oops'
    app.router.add_get(endpoint, send_error)
    client = await aiohttp_client(app)
    await fetch_page(endpoint, [], client, 1, 0)
