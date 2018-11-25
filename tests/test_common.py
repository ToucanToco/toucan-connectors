from toucan_connectors.common import nosql_apply_parameters_to_query


def test_apply_parameter_to_query_do_nothing():
    """
    It should do nothing
    """
    query = [{'$match': {'domain': 'yo', 'cat': 1}}]
    res = nosql_apply_parameters_to_query(query, None)
    assert res == query


def test_apply_parameter_to_query_int_param():
    """
    It should work when a paramters is an int
    """
    query = [{'$match': {'domain': '%(param1)s', 'cat': '%(param2)s'}}]
    parameters = {'param1': 'yo', 'param2': 1}
    expected = [{'$match': {'domain': 'yo', 'cat': 1}}]
    assert nosql_apply_parameters_to_query(query, parameters) == expected


def test_apply_parameter_to_query_in_expression():
    """
    It sould work when a parameter is in an expression (e.g. OData)
    """
    query = {'entity': 'books', 'query': {'$filter': "title eq '%(title)s'"}}
    parameters = {"title": "the overstory"}
    expected = {'entity': 'books', 'query': {'$filter': "title eq 'the overstory'"}}
    assert nosql_apply_parameters_to_query(query, parameters) == expected
