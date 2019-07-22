import pytest

from toucan_connectors.mongo.mongo_translator import MongoExpression


def test_mongo_expression():
    query = ('"a b"=="1" or ((b not in ["la", 5, false]) and (c<1.5) '
             'and (d != null) and (e == true)) or (f in [null, true])'
             'or (g>=2) or (h<=0) or (i>-3.5)')
    expected = {
        '$or': [
            {'a b': '1'},
            {'$and': [
                {'b': {'$nin': ['la', 5, False]}},
                {'c': {'$lt': 1.5}},
                {'d': {'$ne': None}},
                {'e': True}]},
            {'f': {'$in': [None, True]}},
            {'g': {'$gte': 2}},
            {'h': {'$lte': 0}},
            {'i': {'$gt': -3.5}}
        ]
    }

    result = MongoExpression().parse(query)
    assert result == expected


def test_mongo_expression_with_jinja():
    expr = "(type == 'YTD') or (periode == 'yo_{{periode}}')"
    expected = {'$or': [{'type': 'YTD'}, {'periode': 'yo_{{periode}}'}]}
    assert MongoExpression().parse(expr) == expected

    expr = "(type == 'YTD') or (periode == {{periode}})"
    expected = {'$or': [{'type': 'YTD'}, {'periode': '{{periode}}'}]}
    assert MongoExpression().parse(expr) == expected

    expr = "(type == 'YTD') or (periode == 'yo_{{my_indic[\"a\"]}}')"
    expected = {'$or': [{'type': 'YTD'}, {'periode': 'yo_{{my_indic["a"]}}'}]}
    assert MongoExpression().parse(expr) == expected

    expr = "(type == 'YTD') or (periode == '{{my_indic[0]}}')"
    expected = {'$or': [{'type': 'YTD'}, {'periode': '{{my_indic[0]}}'}]}
    assert MongoExpression().parse(expr) == expected


def test_mongo_expression_exception():
    query = '1=="1"'
    with pytest.raises(Exception) as e:
        MongoExpression().parse(query)
    assert 'Missing method for Num' == str(e.value)
