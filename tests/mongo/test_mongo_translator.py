import pytest

from toucan_connectors.mongo.mongo_translator import (
    MongoOperatorMapping,
    permission_condition_to_mongo_clause,
    permission_conditions_to_mongo_query,
)


def test_mongo_operator_mapping_from_identifier():
    assert MongoOperatorMapping.from_identifier('eq') == MongoOperatorMapping.EQUAL
    assert MongoOperatorMapping.from_identifier('ne') == MongoOperatorMapping.NOT_EQUAL
    assert MongoOperatorMapping.from_identifier('lt') == MongoOperatorMapping.LOWER_THAN
    assert MongoOperatorMapping.from_identifier('le') == MongoOperatorMapping.LOWER_THAN_EQUAL
    assert MongoOperatorMapping.from_identifier('gt') == MongoOperatorMapping.GREATER_THAN
    assert MongoOperatorMapping.from_identifier('ge') == MongoOperatorMapping.GREATER_THAN_EQUAL
    assert MongoOperatorMapping.from_identifier('in') == MongoOperatorMapping.IN
    assert MongoOperatorMapping.from_identifier('nin') == MongoOperatorMapping.NOT_IN
    assert MongoOperatorMapping.from_identifier('matches') == MongoOperatorMapping.MATCHES
    assert MongoOperatorMapping.from_identifier('notmatches') == MongoOperatorMapping.NOT_MATCHES
    assert MongoOperatorMapping.from_identifier('isnull') == MongoOperatorMapping.IS_NULL
    assert MongoOperatorMapping.from_identifier('notnull') == MongoOperatorMapping.IS_NOT_NULL


def test_mongo_operator_mapping_to_clause():
    assert MongoOperatorMapping.EQUAL.to_clause('type', 'YTD') == {'type': {'$eq': 'YTD'}}
    assert MongoOperatorMapping.NOT_EQUAL.to_clause('type', 'YTD') == {'type': {'$ne': 'YTD'}}
    assert MongoOperatorMapping.LOWER_THAN.to_clause('type', 'YTD') == {'type': {'$lt': 'YTD'}}
    assert MongoOperatorMapping.LOWER_THAN_EQUAL.to_clause('type', 'YTD') == {
        'type': {'$lte': 'YTD'}
    }
    assert MongoOperatorMapping.GREATER_THAN.to_clause('type', 'YTD') == {'type': {'$gt': 'YTD'}}
    assert MongoOperatorMapping.GREATER_THAN_EQUAL.to_clause('type', 'YTD') == {
        'type': {'$gte': 'YTD'}
    }
    assert MongoOperatorMapping.IN.to_clause('type', ['YTD']) == {'type': {'$in': ['YTD']}}
    assert MongoOperatorMapping.NOT_IN.to_clause('type', ['YTD']) == {'type': {'$nin': ['YTD']}}
    assert MongoOperatorMapping.MATCHES.to_clause('type', 'expr') == {'type': {'$regex': 'expr'}}
    assert MongoOperatorMapping.NOT_MATCHES.to_clause('type', 'expr') == {
        'type': {'$not': {'$regex': 'expr'}}
    }
    assert MongoOperatorMapping.IS_NULL.to_clause('type', 'YTD') == {'type': {'$exists': False}}
    assert MongoOperatorMapping.IS_NOT_NULL.to_clause('type', 'YTD') == {'type': {'$exists': True}}


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
