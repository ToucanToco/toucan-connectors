import pytest
from aiohttp import web

from toucan_connectors.common import (
    NonValidVariable,
    apply_query_parameters,
    fetch,
    nosql_apply_parameters_to_query,
)


def test_apply_parameter_to_query_do_nothing():
    """
    It should do nothing
    """
    query = [{'$match': {'domain': 'yo', 'cat': 1, 'step': '2'}}]
    res = nosql_apply_parameters_to_query(query, None)
    assert res == query


@pytest.mark.parametrize(
    'query,params,expected',
    [
        (
            {'$match': {'domain': 'truc', 'indic': '{{my_indic[0]*my_indic[1]}}'}},
            {'my_indic': [5, 6]},
            {'$match': {'domain': 'truc', 'indic': 30}},
        ),
        (
            {'$match': {'domain': 'truc', 'indic': '{%if my_indic%}1{%else%}2{%endif%}'}},
            {'my_indic': False},
            {'$match': {'domain': 'truc', 'indic': 2}},
        ),
        (
            {
                '$match': {
                    '$and': [
                        {'domain': 'truc', 'indic0': '{{my_indic[0]}}'},
                        {'indic1': '{{my_indic[1]}}', 'indic2': 'yo_{{my_indic[2]}}'},
                        {'indic_list': '{{my_indic}}'},
                    ]
                }
            },
            {'my_indic': ['0', 1, '2']},
            {
                '$match': {
                    '$and': [
                        {'domain': 'truc', 'indic0': '0'},
                        {'indic1': 1, 'indic2': 'yo_2'},
                        {'indic_list': ['0', 1, '2']},
                    ]
                }
            },
        ),
        (
            {
                '$match': {
                    '$and': [
                        {'domain': 'truc', 'indic0': '%(my_indic_0)s'},
                        {'indic1': '%(my_indic_1)s', 'indic2': 'yo_%(my_indic_2)s'},
                        {'indic_list': '%(my_indic)s'},
                    ]
                }
            },
            {'my_indic_0': '0', 'my_indic_1': 1, 'my_indic_2': '2', 'my_indic': ['0', 1, '2']},
            {
                '$match': {
                    '$and': [
                        {'domain': 'truc', 'indic0': '0'},
                        {'indic1': 1, 'indic2': 'yo_2'},
                        {'indic_list': ['0', 1, '2']},
                    ]
                }
            },
        ),
        (
            {
                '$match': {
                    '$and': [
                        {'domain': 'truc', 'indic0': '{{my_indic["zero"]}}'},
                        {'indic1': '{{my_indic["one"]}}', 'indic2': 'yo_{{my_indic["two"]}}'},
                        {'indic_list': '{{my_indic}}'},
                    ]
                }
            },
            {'my_indic': {'zero': '0', 'one': 1, 'two': '2'}},
            {
                '$match': {
                    '$and': [
                        {'domain': 'truc', 'indic0': '0'},
                        {'indic1': 1, 'indic2': 'yo_2'},
                        {'indic_list': {'zero': '0', 'one': 1, 'two': '2'}},
                    ]
                }
            },
        ),
        (
            {'data': '%(fakirQuery)s'},
            {
                'fakirQuery': '[{"values":["bibou"],"chartParam":"test","type":"test","name":"test"}]'
            },
            {'data': '[{"values":["bibou"],"chartParam":"test","type":"test","name":"test"}]'},
        ),
        ({'data': 1}, {}, {'data': 1}),
        ({'data': '1'}, {}, {'data': '1'}),
        (
            {'domain': 'blah', 'country': {'$ne': '%(country)s'}, 'city': '%(city)s'},
            {'city': 'Paris'},
            {'domain': 'blah', 'country': {}, 'city': 'Paris'},
        ),
        (
            [{'$match': {'country': '%(country)s', 'city': 'Test'}}, {'$match': {'b': 1}}],
            {'city': 'Paris'},
            [{'$match': {'city': 'Test'}}, {'$match': {'b': 1}}],
        ),
        ({'code': '%(city)s_%(country)s', 'domain': 'Test'}, {'city': 'Paris'}, {'domain': 'Test'}),
        (
            {'code': '%(city)s_%(country)s', 'domain': 'Test'},
            {'city': 'Paris', 'country': 'France'},
            {'code': 'Paris_France', 'domain': 'Test'},
        ),
        (
            {'domain': 'blah', 'country': {'$ne': '{{country}}'}, 'city': '{{city}}'},
            {'city': 'Paris'},
            {'domain': 'blah', 'country': {}, 'city': 'Paris'},
        ),
        (
            [{'$match': {'country': '{{country["name"]}}', 'city': 'Test'}}, {'$match': {'b': 1}}],
            {'city': 'Paris'},
            [{'$match': {'city': 'Test'}}, {'$match': {'b': 1}}],
        ),
        (
            {'code': '{{city}}_{{country[0]}}', 'domain': 'Test'},
            {'city': 'Paris'},
            {'domain': 'Test'},
        ),
        (
            {'code': '{{city}}_{{country}}', 'domain': 'Test'},
            {'city': 'Paris', 'country': 'France'},
            {'code': 'Paris_France', 'domain': 'Test'},
        ),
        ({'code': '{{city}}_{{country}}', 'domain': 'Test'}, None, {'domain': 'Test'}),
    ],
)
def test_apply_parameter_to_query(query, params, expected):
    assert nosql_apply_parameters_to_query(query, params) == expected


def test_nosql_apply_parameters_to_query_dot():
    """It should handle both `x["y"]` and `x.y`"""
    query1 = {'facet': '{{ facet.value }}', 'sort': '{{ rank[0] }}', 'rows': '{{ bibou[0].value }}'}
    query2 = {
        'facet': '{{ facet["value"] }}',
        'sort': '{{ rank[0] }}',
        'rows': '{{ bibou[0]["value"] }}',
    }
    parameters = {'facet': {'value': 'auteur'}, 'rank': ['rang'], 'bibou': [{'value': 50}]}
    res1 = nosql_apply_parameters_to_query(query1, parameters)
    res2 = nosql_apply_parameters_to_query(query2, parameters)
    assert res1 == res2 == {'facet': 'auteur', 'sort': 'rang', 'rows': 50}


def test_render_raw_permission_no_params():
    query = '(indic0 == 0 or indic1 == 1)'
    assert apply_query_parameters(query, None) == query


def test_render_raw_permission():
    query = (
        '(indic0 == {{my_indic[0]}} or indic1 == {{my_indic[1]}}) and '
        'indic2 == "yo_{{my_indic[2]}}" and indic_list == {{my_indic}}'
    )
    params = {'my_indic': ['0', 1, '2']}
    expected = (
        '(indic0 == "0" or indic1 == 1) and ' 'indic2 == "yo_2" and indic_list == [\'0\', 1, \'2\']'
    )
    assert apply_query_parameters(query, params) == expected


def test_bad_variable_in_query():
    """It should thrown a NonValidEndpointVariable exception if bad variable in endpoint"""
    query = {'url': '/stuff/%(thing)s/foo'}
    params = {}
    nosql_apply_parameters_to_query(query, params)
    with pytest.raises(NonValidVariable) as err:
        nosql_apply_parameters_to_query(query, params, handle_errors=True)
    assert str(err.value) == 'Non valid variable thing'


# fetch tests

FAKE_DATA = {'foo': 'bar', 'baz': 'fudge'}


async def send_200_success(req: web.Request):
    """Send a response with a success."""
    return web.json_response(FAKE_DATA, status=200)


async def send_401_error(req: web.Request) -> dict:
    """Send a response with an error."""
    return web.Response(reason='Unauthorized', status=401)


async def test_fetch_happy(aiohttp_client, loop):
    """It should return a properly-formed dictionary."""
    app = web.Application(loop=loop)
    app.router.add_get('/foo', send_200_success)

    client = await aiohttp_client(app)
    res = await fetch('/foo', client)

    assert res == FAKE_DATA


async def test_fetch_bad_response(aiohttp_client, loop):
    """It should throw an Exception with a message if there is an error."""
    app = web.Application(loop=loop)
    app.router.add_get('/hotels', send_401_error)

    client = await aiohttp_client(app)
    with pytest.raises(Exception) as err:
        await fetch('/hotels', client)

    assert str(err.value) == 'Aborting request due to error from the API: 401, Unauthorized'
