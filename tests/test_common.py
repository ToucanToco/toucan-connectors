from toucan_connectors.common import nosql_apply_parameters_to_query


def test_apply_parameter_to_query():
    parameters = {'param1': 'yo', 'param2': 1}
    query = [{'$match': {'domain': '%(param1)s', 'cat': '%(param2)s'}}]
    expected = [{'$match': {'domain': 'yo', 'cat': 1}}]
    res = nosql_apply_parameters_to_query(query, parameters)
    assert res == expected

    parameters = {'table': 'my_table', 'period': 'semaine'}
    query = 'SELECT * FROM %(table)s WHERE PERIOD=%(period)s'
    expected = 'SELECT * FROM "my_table" WHERE PERIOD="semaine"'
    res = nosql_apply_parameters_to_query(query, parameters)
    assert res == expected

    query = [{'$match': {'domain': 'yo', 'cat': 1}}]
    res = nosql_apply_parameters_to_query(query, None)
    assert res == query
