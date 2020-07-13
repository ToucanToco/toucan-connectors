import pytest

from toucan_connectors.documentdb.documentdb_translator import DocumentDBConditionTranslator


def test_translate_condition_unit_to_mongo_match():
    # works with list
    c = {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']}
    assert DocumentDBConditionTranslator.translate(c) == {'city name': {'$in': ['Paris', 'London']}}
    # works with numbers
    c = {'column': 'population', 'operator': 'eq', 'value': 42}
    assert DocumentDBConditionTranslator.translate(c) == {'population': {'$eq': 42}}
    # works with strings
    c = {'column': 'country', 'operator': 'eq', 'value': 'France'}
    assert DocumentDBConditionTranslator.translate(c) == {'country': {'$eq': 'France'}}
    # raise when needed
    with pytest.raises(ValueError):
        DocumentDBConditionTranslator.translate({'column': 'population', 'operator': 'eq'})
    with pytest.raises(ValueError):
        DocumentDBConditionTranslator.translate({'column': 'population', 'value': 42})
    with pytest.raises(ValueError):
        DocumentDBConditionTranslator.translate({'operator': 'eq', 'value': 42})
    with pytest.raises(ValueError):
        DocumentDBConditionTranslator.translate(
            {'column': 'population', 'operator': 'unsupported', 'value': 42}
        )


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
    assert DocumentDBConditionTranslator.translate(c) == {
        '$and': [
            {'country': {'$eq': 'France'}},
            {'$or': [{'city name': {'$in': ['Paris', 'London']}}, {'population': {'$eq': 42}}]},
        ]
    }
    # Invalid and/or condition list
    with pytest.raises(ValueError):
        DocumentDBConditionTranslator.translate({'and': 1})
    with pytest.raises(ValueError):
        DocumentDBConditionTranslator.translate({'or': 1})
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

    result = DocumentDBConditionTranslator.translate(query)
    assert result == expected


def test_DocumentDBConditionTranslator_translate_with_jinja():
    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': 'yo_{{periode}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': 'yo_{{periode}}'}}]}
    assert DocumentDBConditionTranslator.translate(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': '{{periode}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': '{{periode}}'}}]}
    assert DocumentDBConditionTranslator.translate(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': 'yo_{{my_indic[\"a\"]}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': 'yo_{{my_indic["a"]}}'}}]}
    assert DocumentDBConditionTranslator.translate(expr) == expected

    expr = {
        'or': [
            {'column': 'type', 'operator': 'eq', 'value': 'YTD'},
            {'column': 'periode', 'operator': 'eq', 'value': '{{my_indic[0]}}'},
        ]
    }
    expected = {'$or': [{'type': {'$eq': 'YTD'}}, {'periode': {'$eq': '{{my_indic[0]}}'}}]}
    assert DocumentDBConditionTranslator.translate(expr) == expected


def test_DocumentDBConditionTranslator_operators():
    assert DocumentDBConditionTranslator.EQUAL('col', 'val') == {'col': {'$eq': 'val'}}
    assert DocumentDBConditionTranslator.NOT_EQUAL('col', 'val') == {'col': {'$ne': 'val'}}
    assert DocumentDBConditionTranslator.GREATER_THAN('col', 3) == {'col': {'$gt': 3}}
    assert DocumentDBConditionTranslator.GREATER_THAN_EQUAL('col', 3) == {'col': {'$gte': 3}}
    assert DocumentDBConditionTranslator.LOWER_THAN('col', 3) == {'col': {'$lt': 3}}
    assert DocumentDBConditionTranslator.LOWER_THAN_EQUAL('col', 3) == {'col': {'$lte': 3}}
    assert DocumentDBConditionTranslator.IN('col', ['val']) == {'col': {'$in': ['val']}}
    assert DocumentDBConditionTranslator.NOT_IN('col', ['val']) == {'col': {'$nin': ['val']}}
    assert DocumentDBConditionTranslator.IS_NULL('col') == {'col': {'$exists': False}}
    assert DocumentDBConditionTranslator.IS_NOT_NULL('col') == {'col': {'$exists': True}}
    assert DocumentDBConditionTranslator.MATCHES('col', 'val') == {'col': {'$regex': 'val'}}
    assert DocumentDBConditionTranslator.NOT_MATCHES('col', 'val') == {
        'col': {'$not': {'$regex': 'val'}}
    }
