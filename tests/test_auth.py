import responses

from toucan_connectors.auth import Auth, CustomTokenServer


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
