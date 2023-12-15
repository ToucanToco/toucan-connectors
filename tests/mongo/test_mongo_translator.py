import pytest

from toucan_connectors.mongo.mongo_translator import MongoConditionTranslator


def test_translate_condition_unit_to_mongo_match():
    # works with list
    c = {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']}
    assert MongoConditionTranslator.translate(c) == {'city name': {'$in': ['Paris', 'London']}}
    # works with numbers
    c = {'column': 'population', 'operator': 'eq', 'value': 42}
    assert MongoConditionTranslator.translate(c) == {'population': {'$eq': 42}}
    # works with strings
    c = {'column': 'country', 'operator': 'eq', 'value': 'France'}
    assert MongoConditionTranslator.translate(c) == {'country': {'$eq': 'France'}}
    # raise when needed
    with pytest.raises(ValueError):
        MongoConditionTranslator.translate({'column': 'population', 'operator': 'eq'})
    with pytest.raises(ValueError):
        MongoConditionTranslator.translate({'column': 'population', 'value': 42})
    with pytest.raises(ValueError):
        MongoConditionTranslator.translate({'operator': 'eq', 'value': 42})
    with pytest.raises(ValueError):
        MongoConditionTranslator.translate({'column': 'population', 'operator': 'unsupported', 'value': 42})


def test_translate_condition_to_mongo_match():
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
    assert MongoConditionTranslator.translate(c) == {
        '$and': [
            {'country': {'$eq': 'France'}},
            {'$or': [{'city name': {'$in': ['Paris', 'London']}}, {'population': {'$eq': 42}}]},
        ]
    }
    # Invalid and/or condition list
    with pytest.raises(ValueError):
        MongoConditionTranslator.translate({'and': 1})
    with pytest.raises(ValueError):
        MongoConditionTranslator.translate({'or': 1})
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
            {'column': 'j', 'operator': 'isnull', 'value': None},
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
            {'j': {'$exists': False}},
        ]
    }

    result = MongoConditionTranslator.translate(query)
    assert result == expected


def test_MongoConditionTranslator_translate_with_jinja():
    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': 'yo_{{periode}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': 'yo_{{periode}}'}}]}
    assert MongoConditionTranslator.translate(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': '{{periode}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': '{{periode}}'}}]}
    assert MongoConditionTranslator.translate(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': 'yo_{{my_indic["a"]}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': 'yo_{{my_indic["a"]}}'}}]}
    assert MongoConditionTranslator.translate(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': '{{my_indic[0]}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': '{{my_indic[0]}}'}}]}
    assert MongoConditionTranslator.translate(expr) == expected


def test_MongoConditionTranslator_operators():
    assert MongoConditionTranslator.EQUAL('col', 'val') == {'col': {'$eq': 'val'}}
    assert MongoConditionTranslator.NOT_EQUAL('col', 'val') == {'col': {'$ne': 'val'}}
    assert MongoConditionTranslator.GREATER_THAN('col', 3) == {'col': {'$gt': 3}}
    assert MongoConditionTranslator.GREATER_THAN_EQUAL('col', 3) == {'col': {'$gte': 3}}
    assert MongoConditionTranslator.LOWER_THAN('col', 3) == {'col': {'$lt': 3}}
    assert MongoConditionTranslator.LOWER_THAN_EQUAL('col', 3) == {'col': {'$lte': 3}}
    assert MongoConditionTranslator.IN('col', ['val']) == {'col': {'$in': ['val']}}
    assert MongoConditionTranslator.NOT_IN('col', ['val']) == {'col': {'$nin': ['val']}}
    assert MongoConditionTranslator.IS_NULL('col') == {'col': {'$exists': False}}
    assert MongoConditionTranslator.IS_NOT_NULL('col') == {'col': {'$exists': True}}
    assert MongoConditionTranslator.MATCHES('col', 'val') == {'col': {'$regex': 'val'}}
    assert MongoConditionTranslator.NOT_MATCHES('col', 'val') == {'col': {'$not': {'$regex': 'val'}}}
