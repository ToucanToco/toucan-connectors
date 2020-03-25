import pytest

from toucan_connectors.mongo.mongo_translator import (
    permission_condition_to_mongo_clause,
    permission_conditions_to_mongo_query,
)


def test_permission_condition_to_mongo_clause():
    # works with list
    c = {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']}
    assert permission_condition_to_mongo_clause(c) == {'city name': {'$in': ['Paris', 'London']}}
    # works with numbers
    c = {'column': 'population', 'operator': 'eq', 'value': 42}
    assert permission_condition_to_mongo_clause(c) == {'population': {'$eq': 42}}
    # works with strings
    c = {'column': 'country', 'operator': 'eq', 'value': 'France'}
    assert permission_condition_to_mongo_clause(c) == {'country': {'$eq': 'France'}}
    # raise when needed
    with pytest.raises(KeyError):
        permission_condition_to_mongo_clause({'column': 'population', 'operator': 'eq'})
    with pytest.raises(KeyError):
        permission_condition_to_mongo_clause({'column': 'population', 'value': 42})
    with pytest.raises(KeyError):
        permission_condition_to_mongo_clause({'operator': 'eq', 'value': 42})
    with pytest.raises(ValueError):
        permission_condition_to_mongo_clause(
            {'column': 'population', 'operator': 'unsupported', 'value': 42}
        )


def test_permission_conditions_to_mongo_query():
    c = {
        'and': [
            {'column': 'country', 'operator': 'eq', 'value': 'France'},
            {
                'or': [
                    {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']},
                    {'column': 'population', 'operator': 'eq', 'value': 42},
                ]
            },
        ]
    }
    assert permission_conditions_to_mongo_query(c) == {
        '$and': [
            {'country': {'$eq': 'France'}},
            {'$or': [{'city name': {'$in': ['Paris', 'London']}}, {'population': {'$eq': 42}}]},
        ]
    }
    # Invalid and/or condition list
    with pytest.raises(ValueError):
        permission_conditions_to_mongo_query({'and': 1})
    with pytest.raises(ValueError):
        permission_conditions_to_mongo_query({'or': 1})
    # Complicated one
    query = {
        'or': [
            {
                'and': [
                    {'column': 'b', 'operator': 'nin', 'value': ['la', 5, False]},
                    {'column': 'c', 'operator': 'lt', 'value': 1.5},
                    {'column': 'd', 'operator': 'ne', 'value': None},
                    {'column': 'e', 'operator': 'eq', 'value': True},
                ]
            },
            {'column': 'f', 'operator': 'in', 'value': [None, True]},
            {'column': 'g', 'operator': 'ge', 'value': 2},
            {'column': 'h', 'operator': 'le', 'value': 0},
            {'column': 'i', 'operator': 'gt', 'value': -3.5},
        ]
    }
    expected = {
        '$or': [
            {
                '$and': [
                    {'b': {'$nin': ['la', 5, False]}},
                    {'c': {'$lt': 1.5}},
                    {'d': {'$ne': None}},
                    {'e': {'$eq': True}},
                ]
            },
            {'f': {'$in': [None, True]}},
            {'g': {'$gte': 2}},
            {'h': {'$lte': 0}},
            {'i': {'$gt': -3.5}},
        ]
    }

    result = permission_conditions_to_mongo_query(query)
    assert result == expected


def test_permission_conditions_to_mongo_query_with_jinja():
    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': 'yo_{{periode}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': 'yo_{{periode}}'}}]}
    assert permission_conditions_to_mongo_query(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': '{{periode}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': '{{periode}}'}}]}
    assert permission_conditions_to_mongo_query(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': 'yo_{{my_indic[\"a\"]}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': 'yo_{{my_indic["a"]}}'}}]}
    assert permission_conditions_to_mongo_query(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': '{{my_indic[0]}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': '{{my_indic[0]}}'}}]}
    assert permission_conditions_to_mongo_query(expr) == expected
