from aiohttp import web
from toucan_connectors.aircall.aircall_connector import fetch_page


async def send_no_link(req):
    fake_response = {
        'data' : {
            'stuff' : 'stuff'
        },
        'meta' : {
            'next_page_link' : None
        }
    }
    return web.json_response(fake_response)


async def send_next_link(req):
    fake_response = {
        'data' : {
            'stuff' : 'stuff'
        },
        'meta' : {
            'next_page_link' : 'https://api.aircall.io/v1/foo?page=2'
        }
    }

    return web.json_response(fake_response)


async def send_no_meta(req):
    fake_response = {
        'data' : {
            'stuff' : 'stuff'
        }
    }
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
    assert res_dict.get('data', None) is not None
    assert res_dict.get('meta', None) is not None


async def test_fetch_page_with_low_limit(aiohttp_client, loop):
    """Test fetch_page to see limit and current pass stopping recursion"""
    app = web.Application(loop=loop)
    endpoint = '/foo'
    app.router.add_get(endpoint, send_next_link)
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
    res = await fetch_page(endpoint, [], client, 1, 0)
    assert len(res) == 1
