import pytest
from _pyjq import ScriptRuntimeError
from aiohttp import web
from pydantic import ValidationError

import tests.general_helpers as helpers
from toucan_connectors.revinate.revinate_connector import (
    RevinateAuthentication,
    RevinateConnector,
    RevinateDataSource,
    fetch,
)

fetch_fn_name = 'toucan_connectors.revinate.revinate_connector.fetch'


@pytest.fixture
def authentication():
    return RevinateAuthentication(
        api_key='abc123efg',
        api_secret='b7536617fa4a1a9e3c7a707abcde866771570cb8c9a28401abcde755b48be6cb',
        username='mr.smith@matrix.net',
    )


@pytest.fixture
def ds():
    return RevinateDataSource(name='test', domain='test_domain', endpoint='hotels')


@pytest.fixture
def base_connector(authentication):
    return RevinateConnector(authentication=authentication, name='Test Connector')


FAKE_DATA = {
    'links': [{'rel': '', 'href': '', 'templated': False}],
    'content': [
        {
            'name': 'Hotel of Pikachus',
            'slug': '',
            'logo': 'pikachu-hotel.png',
            'url': '/hotel-of-pikachus',
            'address1': '13 13th Street',
            'address2': '',
            'city': 'Some City',
            'state': 'Some State',
            'postalCode': '131313',
            'country': 'Some Country',
            'tripAdvisorId': 13,
            'revinatePurchaseUri': '',
            'revinateLoginUri': '',
            'accountType': '',
            'links': [{'rel': '', 'href': '', 'templated': False}],
        }
    ],
    'page': {'size': 50, 'totalElements': 498, 'totalPages': 10, 'number': 2},
}

JQ_FILTERED_DATA = [
    {
        'name': 'Hotel of Pikachus',
        'slug': '',
        'logo': 'pikachu-hotel.png',
        'url': '/hotel-of-pikachus',
        'address1': '13 13th Street',
        'address2': '',
        'city': 'Some City',
        'state': 'Some State',
        'postalCode': '131313',
        'country': 'Some Country',
        'tripAdvisorId': 13,
        'revinatePurchaseUri': '',
        'revinateLoginUri': '',
        'accountType': '',
        'links': [{'rel': '', 'href': '', 'templated': False}],
    }
]


@pytest.mark.asyncio
async def test__get_data_happy_case(base_connector, ds, mocker):
    """It should return valid data if everything is valid"""
    fake_fetch = mocker.patch(fetch_fn_name, return_value=helpers.build_future(FAKE_DATA),)

    fake_endpoint = f'/{ds.endpoint}?page=2&size=50'

    res = await base_connector._get_data(fake_endpoint, jq_filter='.')

    assert res == JQ_FILTERED_DATA
    assert fake_fetch.call_count == 1


@pytest.mark.asyncio
async def test__get_data_w_bad_jq_filter(base_connector, ds, mocker):
    """It should throw a ValueError if jq filter is not valid"""
    fake_fetch = mocker.patch(fetch_fn_name, return_value=helpers.build_future(FAKE_DATA))

    with pytest.raises(ValueError):
        await base_connector._get_data(f'/{ds.endpoint}', 'putzes')

    assert fake_fetch.call_count == 1


@pytest.mark.asyncio
async def test__get_data_w_incorrect_filter(base_connector, ds, mocker):
    """It should return an array with None if the jq filter does not match the data"""
    fake_fetch = mocker.patch(fetch_fn_name, return_value=helpers.build_future(FAKE_DATA))

    with pytest.raises(ScriptRuntimeError):
        await base_connector._get_data(f'/{ds.endpoint}', '.putzes')

    assert fake_fetch.call_count == 1


@pytest.mark.asyncio
async def test__get_data_w_exception(base_connector, ds, mocker):
    """It should raise an Exception if there's been an exception in the previous function"""
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=Exception)

    with pytest.raises(Exception):
        await base_connector._get_data(f'{ds.endpoint}', '.')

    assert fake_fetch.call_count == 1


@pytest.mark.asyncio
async def test__get_data_w_no_content(base_connector, ds, mocker):
    """It should still succeed even with an empty content array"""
    mocker.patch(
        fetch_fn_name,
        return_value=helpers.build_future(
            {
                'links': [{'rel': '', 'href': '', 'templated': False}],
                'content': [],
                'page': {'size': 50, 'totalElements': 498, 'totalPages': 10, 'number': 2},
            }
        ),
    )

    result = await base_connector._get_data(f'/{ds.endpoint}', '.')

    assert len(result) == 0


def test__run_fetch(base_connector, ds, mocker):
    """It should call _run_fetch and produce a dict"""
    fake__get_data = mocker.patch.object(
        RevinateConnector, '_get_data', return_value=helpers.build_future(JQ_FILTERED_DATA)
    )

    result = base_connector._run_fetch(f'/{ds.endpoint}', '.')

    assert result == JQ_FILTERED_DATA
    assert fake__get_data.call_count == 1


def test__retrieve_data_happy_case(base_connector, ds, mocker):
    """It should return a correctly formatted pandas dataframe"""
    fake__run_fetch = mocker.patch.object(
        RevinateConnector, '_run_fetch', return_value=JQ_FILTERED_DATA
    )
    df = base_connector._retrieve_data(data_source=ds)

    assert fake__run_fetch.call_count == 1
    assert df.shape == (1, 15)


def test__retrieve_data_exception_case(base_connector, ds, mocker):
    """It should pass on an Exception or some other error"""
    fake_fetch = mocker.patch(fetch_fn_name, side_effect=ScriptRuntimeError)

    with pytest.raises(ScriptRuntimeError):
        base_connector._retrieve_data(ds)

    assert fake_fetch.call_count == 1


def test__retrieve_data_bad_connector(authentication, ds, mocker):
    """It should raise an error if connector or authentication not properly formed"""
    fake__run_fetch = mocker.patch.object(
        RevinateConnector, '_run_fetch', return_value=JQ_FILTERED_DATA
    )

    # case where authentication is not properly formed
    with pytest.raises(ValidationError):
        RevinateConnector(
            authentication=RevinateAuthentication(api_key='abc123efg', api_secret='stuff'),
            name='Bad Connector 1',
        )._retrieve_data(ds)

    # case where authentication is missing
    with pytest.raises(ValidationError):
        RevinateConnector(name='Bad Connector 3')._retrieve_data(ds)

    # failure should happen before the connector's _retrieve_data method is called
    assert fake__run_fetch.call_count == 0


def test__retrieve_data_empty_content(base_connector, ds, mocker):
    """It should return an empty dataframe but still succeed"""
    fake__run_fetch = mocker.patch.object(RevinateConnector, '_run_fetch', return_value=[])

    df = base_connector._retrieve_data(ds)
    assert df.shape == (0, 0)
    assert fake__run_fetch.call_count == 1


# Fetch function tests


async def send_200_success(req: web.Request):
    """Sends a response with a success"""
    return web.json_response(FAKE_DATA, status=200)


async def send_401_error(req: web.Request) -> dict:
    """Sends a response with an error"""
    return web.Response(reason='Unauthorized', status=401)


async def test_fetch_happy(aiohttp_client, loop):
    """It should return a properly-formed dictionary"""
    app = web.Application(loop=loop)
    app.router.add_get('/hotels', send_200_success)

    client = await aiohttp_client(app)
    res = await fetch('/hotels', client)

    assert res == FAKE_DATA


async def test_fetch_bad_response(aiohttp_client, loop):
    """It should throw an Exception with a message if there is an error from Revinate"""
    app = web.Application(loop=loop)
    app.router.add_get('/hotels', send_401_error)

    client = await aiohttp_client(app)
    with pytest.raises(Exception) as err:
        await fetch('/hotels', client)

    assert (
        str(err.value) == 'Aborting Revinate request due to error from their API: 401 Unauthorized'
    )
