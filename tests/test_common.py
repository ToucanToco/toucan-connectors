from datetime import datetime, timedelta

import pytest
from aiohttp import web

from toucan_connectors.common import (
    ConnectorStatus,
    NonValidVariable,
    adapt_param_type,
    apply_query_parameters,
    convert_to_qmark_paramstyle,
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
        (
            {'column': 'date', 'operator': 'eq', 'value': '{{ t + delta }}'},
            {'t': datetime(2020, 12, 31), 'delta': timedelta(days=1)},
            {'column': 'date', 'operator': 'eq', 'value': datetime(2021, 1, 1)},
        ),
        (
            {'column': 'date', 'operator': 'eq', 'value': '{{ t.strftime("%d/%m/%Y") }}'},
            {'t': datetime(2020, 12, 31)},
            {'column': 'date', 'operator': 'eq', 'value': '31/12/2020'},
        ),
        (
            {'column': 'date', 'operator': 'in', 'value': '{{ allowed_dates }}'},
            {'allowed_dates': [datetime(2020, 12, 31), datetime(2021, 1, 1)]},
            {
                'column': 'date',
                'operator': 'in',
                'value': [datetime(2020, 12, 31), datetime(2021, 1, 1)],
            },
        ),
        # _flatten_rendered_nested_list tests below:
        (
            {'array': ['{{ entity_id }}', '{{ entity_array }}']},
            {'entity_id': '1', 'entity_array': ['2']},
            {'array': ['1', '2']},
        ),
        (
            {'deep': ['{{ entity_array }}', ['4', '5']]},
            {'entity_array': ['2', '3']},
            {'deep': ['2', '3', ['4', '5']]},
        ),
        (
            {'mixed': ['{{ entity_id }}', '{{ entity_array }}']},
            {'entity_id': 1, 'entity_array': [True]},
            {'mixed': [1, True]},
        ),
    ],
)
def test_nosql_apply_parameters_to_query(query, params, expected):
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


def test_connector_status():
    """
    It should be exported as dict
    """
    assert ConnectorStatus(status=True).to_dict() == {
        'status': True,
        'message': None,
        'error': None,
        'details': [],
    }


@pytest.mark.parametrize(
    'query, params, expected_query, expected_ordered_values',
    [
        (
            'select * from test where id > %(id_nb)s and price > %(price)s;',
            {'id_nb': 1, 'price': 10},
            'select * from test where id > ? and price > ?;',
            [1, 10],
        ),
        (
            'select * from test where id > %(id_nb)s and id < %(id_nb)s + 1;',
            {'id_nb': 1},
            'select * from test where id > ? and id < ? + 1;',
            [1, 1],
        ),
        (
            'select * from test where id > %(id_nb)s and price > %(price)s;',
            {'id_nb': 1},
            'select * from test where id > ? and price > ?;',
            [1, None],
        ),
        (
            'select * from inventory where quantity in %(quantities)s;',
            {'quantities': [150, 154]},
            'select * from inventory where quantity in (?,?);',
            [150, 154],
        ),
    ],
)
def test_convert_pyformat_to_qmark(query, params, expected_query, expected_ordered_values):
    """It should return query in qmark paramstyle and values of extracted params"""
    converted_query, ordered_values = convert_to_qmark_paramstyle(query, params)
    assert ordered_values == expected_ordered_values
    assert converted_query == expected_query


def test_adapt_param_type():
    assert adapt_param_type({'test': [1, 2], 'id': 1}) == {'test': (1, 2), 'id': 1}
