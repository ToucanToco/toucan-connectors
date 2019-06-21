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
