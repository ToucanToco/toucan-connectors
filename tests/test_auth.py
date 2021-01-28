from datetime import datetime, timedelta

import jwt
import responses

from toucan_connectors.auth import Auth, CustomTokenServer


@responses.activate
def test_custom_token_server():
    auth = Auth(
        type='custom_token_server',
        args=['POST', 'https://example.com'],
        kwargs={'data': {'user': 'u', 'password ': 'p'}, 'filter': '.token'},
    )

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
    session = Auth(
        type='custom_token_server',
        args=['POST', 'https://example.com'],
        kwargs={'auth': {'type': 'basic', 'args': ['u', 'p']}},
    ).get_session()

    session.auth(TMP())
    assert responses.calls[1].request.headers['Authorization'].startswith('Basic')


@responses.activate
def test_oauth2_oidc():
    # Case 1: id_token is valid and not expired
    id_token = jwt.encode(
        {'exp': (datetime.now() + timedelta(seconds=10000)).timestamp(), 'user': 'babar'}, key='key'
    )
    auth = Auth(
        type='oauth2_oidc',
        args=[],
        kwargs={
            'id_token': id_token,
            'client_id': 'example_client_id',
            'refresh_token': jwt.encode(
                {'exp': (datetime.now() + timedelta(seconds=100000)).timestamp()}, key='refreshkey'
            ),
            'client_secret': 'example_client_secret',
            'token_endpoint': 'https://example.com/token',
            'refresh': True,
        },
    )
    session = auth.get_session()
    assert session.headers['Authorization'] == f'Bearer {id_token}'

    # Case 2: id_token is expired but refresh token is not
    id_token = jwt.encode(
        {'exp': (datetime.now() - timedelta(seconds=100)).timestamp(), 'aud': 'babar'}, key='key'
    )
    auth.kwargs['id_token'] = id_token
    responses.add(
        method=responses.POST,
        url='https://example.com/token',
        json={'id_token': 'coucou'},
    )
    session = auth.get_session()
