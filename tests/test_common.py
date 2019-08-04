from typing import Union

import pytest
from pydantic import BaseModel

from toucan_connectors.common import (
    create_templated_model,
    nosql_apply_parameters_to_query,
    render_raw_permissions
)


def test_apply_parameter_to_query_do_nothing():
    """
    It should do nothing
    """
    query = [{'$match': {'domain': 'yo', 'cat': 1}}]
    res = nosql_apply_parameters_to_query(query, None)
    assert res == query


def test_apply_parameter_to_query():
    """
    It sould work render all parameters
    """
    tests = [
        ({'$match': {'domain': 'truc', 'indic': '{{my_indic[0]*my_indic[1]}}'}},  # query
         {'my_indic': [5, 6]},  # params
         {'$match': {'domain': 'truc', 'indic': 30}}),  # expected

        ({'$match': {'domain': 'truc', 'indic': '{%if my_indic%}1{%else%}2{%endif%}'}},
         {'my_indic': False},
         {'$match': {'domain': 'truc', 'indic': 2}}),

        ({'$match': {'$and': [
            {'domain': 'truc', 'indic0': '{{my_indic[0]}}'},
            {'indic1': '{{my_indic[1]}}', 'indic2': 'yo_{{my_indic[2]}}'},
            {'indic_list': '{{my_indic}}'}
         ]}},
         {'my_indic': ['0', 1, '2']},
         {'$match': {'$and': [
             {'domain': 'truc', 'indic0': '0'},
             {'indic1': 1, 'indic2': 'yo_2'},
             {'indic_list': ['0', 1, '2']}
         ]}}),

        ({'$match': {'$and': [
            {'domain': 'truc', 'indic0': '%(my_indic_0)s'},
            {'indic1': '%(my_indic_1)s', 'indic2': 'yo_%(my_indic_2)s'},
            {'indic_list': '%(my_indic)s'}
         ]}},
         {'my_indic_0': '0', 'my_indic_1': 1, 'my_indic_2': '2', 'my_indic': ['0', 1, '2']},
         {'$match': {'$and': [
             {'domain': 'truc', 'indic0': '0'},
             {'indic1': 1, 'indic2': 'yo_2'},
             {'indic_list': ['0', 1, '2']}
         ]}}),

        ({'$match': {'$and': [
            {'domain': 'truc', 'indic0': '{{my_indic["zero"]}}'},
            {'indic1': '{{my_indic["one"]}}', 'indic2': 'yo_{{my_indic["two"]}}'},
            {'indic_list': '{{my_indic}}'}
        ]}},
         {'my_indic': {'zero': '0', 'one': 1, 'two': '2'}},
         {'$match': {'$and': [
             {'domain': 'truc', 'indic0': '0'},
             {'indic1': 1, 'indic2': 'yo_2'},
             {'indic_list': {'zero': '0', 'one': 1, 'two': '2'}}
         ]}})
    ]
    for (query, params, expected) in tests:
        assert nosql_apply_parameters_to_query(query, params) == expected


def test_apply_params_with_missing_param():
    tests = [
        ({'domain': 'blah', 'country': {'$ne': '%(country)s'},  # query
          'city': '%(city)s'},
         {'city': 'Paris'},  # params
         {'domain': 'blah', 'country': {}, 'city': 'Paris'}),  # expected

        ([{'$match': {'country': '%(country)s', 'city': 'Test'}},
          {'$match': {'b': 1}}],
         {'city': 'Paris'},
         [{'$match': {'city': 'Test'}}, {'$match': {'b': 1}}]),

        ({'code': '%(city)s_%(country)s', 'domain': 'Test'},
         {'city': 'Paris'},
         {'domain': 'Test'}),

        ({'code': '%(city)s_%(country)s', 'domain': 'Test'},
         {'city': 'Paris', 'country': 'France'},
         {'code': 'Paris_France', 'domain': 'Test'}),

        ({'domain': 'blah', 'country': {'$ne': '{{country}}'},
          'city': '{{city}}'},
         {'city': 'Paris'},
         {'domain': 'blah', 'country': {}, 'city': 'Paris'}),

        ([{'$match': {'country': '{{country["name"]}}', 'city': 'Test'}},
          {'$match': {'b': 1}}],
         {'city': 'Paris'},
         [{'$match': {'city': 'Test'}}, {'$match': {'b': 1}}]),

        ({'code': '{{city}}_{{country[0]}}', 'domain': 'Test'},
         {'city': 'Paris'},
         {'domain': 'Test'}),

        ({'code': '{{city}}_{{country}}', 'domain': 'Test'},
         {'city': 'Paris', 'country': 'France'},
         {'code': 'Paris_France', 'domain': 'Test'}),

        ({'code': '{{city}}_{{country}}', 'domain': 'Test'},
         None,
         {'domain': 'Test'})
    ]
    for (query, params, expected) in tests:
        assert nosql_apply_parameters_to_query(query, params) == expected


def test_nosql_apply_parameters_to_query_dot():
    """It should handle both `x["y"]` and `x.y`"""
    query1 = {
        'facet': '{{ facet.value }}',
        'sort': '{{ rank[0] }}',
        'rows': '{{ bibou[0].value }}'
    }
    query2 = {
        'facet': '{{ facet["value"] }}',
        'sort': '{{ rank[0] }}',
        'rows': '{{ bibou[0]["value"] }}'
    }
    parameters = {
        'facet': {'value': 'auteur'},
        'rank': ['rang'],
        'bibou': [{'value': 50}]
    }
    res1 = nosql_apply_parameters_to_query(query1, parameters)
    res2 = nosql_apply_parameters_to_query(query2, parameters)
    assert res1 == res2 == {
        'facet': 'auteur',
        'sort': 'rang',
        'rows': 50
    }


def test_render_raw_permission_no_params():
    query = '(indic0 == 0 or indic1 == 1)'
    assert render_raw_permissions(query, None) == query


def test_render_raw_permission():
    query = '(indic0 == {{my_indic[0]}} or indic1 == {{my_indic[1]}}) and ' \
            'indic2 == "yo_{{my_indic[2]}}" and indic_list == {{my_indic}}'
    params = {'my_indic': ['0', 1, '2']}
    expected = '(indic0 == "0" or indic1 == 1) and ' \
               'indic2 == "yo_2" and indic_list == [\'0\', 1, \'2\']'
    assert render_raw_permissions(query, params) == expected


class PikaModel(BaseModel):
    a: Union[dict, list]
    b: int
    c: int = 3
    d: str


def test_create_templated_model_basic():
    m = create_templated_model(PikaModel, ['a'])
    assert m.__name__ == 'TemplatedPikaModel'
    assert {
        field_name: f.type_
        for field_name, f in m.__fields__.items()
    } == {
        'a': Union[str, dict, list],
        'b': int,
        'c': int,
        'd': str
    }

    # 'a' can now be a string for the `m`
    with pytest.raises(ValueError):
        PikaModel(a='qwe', b=2, d='d')
    res = m(a='qwe', b=2, d='d')
    assert res.c == 3

    # The rest should not be possible
    with pytest.raises(ValueError):
        PikaModel(a=[1], b='qwe', d='d')
    with pytest.raises(ValueError):
        m(a=[1], b='qwe', d='d')


def test_create_templated_model_unknown_parameters():
    m = create_templated_model(PikaModel, ['a', 'c', 'd', 'e'])
    assert {
        field_name: f.type_
        for field_name, f in m.__fields__.items()
    } == {
        'a': Union[str, dict, list],
        'b': int,
        'c': Union[str, int],
        'd': str
    }
    res = m(a='qwe', b=2, d='d')
    assert res.c == 3
