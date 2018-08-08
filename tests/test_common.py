from toucan_connectors.common import apply_parameters_to_query


def test_apply_parameter_to_query():
    parameters = {'param1': 'yo', 'param2': 1}
    query = "SELECT * FROM blah WHERE name == %(param1)s and %(param2)s"
    expected = 'SELECT * FROM blah WHERE name == "yo" and 1'
    res = apply_parameters_to_query(query, parameters)
    assert res == expected

    query = [{'$match': {'domain': '%(param1)s', 'cat': '%(param2)s'}}]
    expected = [{'$match': {'domain': 'yo', 'cat': 1}}]
    res = apply_parameters_to_query(query, parameters)
    assert res == expected
