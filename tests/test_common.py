import responses

from toucan_connectors.common import Auth, CustomTokenServer, nosql_apply_parameters_to_query


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
    query = {'entity': 'books', 'query': {
                '$filter': "title eq '%(title)s'",
                '$top': "%(top)s"}}
    parameters = {"title": "the overstory", "top": 3}
    expected = {'entity': 'books', 'query': {
                    '$filter': "title eq 'the overstory'",
                    '$top': 3}}
    assert nosql_apply_parameters_to_query(query, parameters) == expected


@responses.activate
def test_custom_token_server():
    auth = Auth(type='custom_token_server',
                args=['POST', 'https://example.com'],
                kwargs={'data': {'user': 'u', 'password ': 'p'}, 'filter': '.token'})

    session = auth.get_session()
    assert isinstance(session.auth, CustomTokenServer)

    responses.add(responses.POST, 'https://example.com', json={'token': 'a'})

    # dummy request class just to introspect headers
    class TMP:
        headers = {}

    session.auth(TMP())
    assert TMP.headers['Authorization'] == 'Bearer a'

    # custom_token_server with its own auth class
    responses.add(responses.POST, 'https://example.com', json={'token': 'a'})
    session = Auth(type='custom_token_server',
                   args=['POST', 'https://example.com'],
                   kwargs={'auth': {'type': 'basic', 'args': ['u', 'p']}}).get_session()

    session.auth(TMP())
    assert responses.calls[1].request.headers['Authorization'].startswith('Basic')
