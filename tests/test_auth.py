from datetime import datetime, timedelta

import jwt
import responses

from toucan_connectors.auth import Auth, AuthType, CustomTokenServer


class DummyRequest:
    headers = {}


@responses.activate
def test_custom_token_server():
    auth = Auth(
        type="custom_token_server",
        args=["POST", "https://example.com"],
        kwargs={"data": {"user": "u", "password ": "p"}, "filter": ".token"},
    )

    session = auth.get_session()
    assert isinstance(session.auth, CustomTokenServer)

    responses.add(responses.POST, "https://example.com", json={"token": "a"})

    session.auth(DummyRequest())
    assert DummyRequest.headers["Authorization"] == "Bearer a"


@responses.activate
def test_custom_token_server_custom_auth_scheme():
    """
    RFC 2617 standard on HTTP Authentication states that credentials should
    consist of at least a scheme and arbitrary parameters:
        credentials = auth-scheme #auth-param

    In practice auth-scheme is often "Bearer" because it is used in the OAuth2 standard.
    This is our default. With the custom_token_server the initial request's response will be
    injected in the the Authorization header after the Bearer auth-scheme.

    This test show that we support altenative auth-schemes, in this case the `filter`
    should emit the complete value of the Authorization header.
    """
    auth = Auth(
        type="custom_token_server",
        args=["POST", "https://example.com"],
        kwargs={
            "data": {"user": "u", "password ": "p"},
            "filter": '"AnaplanAuthToken \(.token)"',  # noqa: W605
        },
    )

    session = auth.get_session()
    assert isinstance(session.auth, CustomTokenServer)

    responses.add(responses.POST, "https://example.com", json={"token": "a"})

    session.auth(DummyRequest())
    assert DummyRequest.headers["Authorization"] == "AnaplanAuthToken a"


@responses.activate
def test_custom_token_server_custom_auth_scheme_and_header_name():
    auth = Auth(
        type="custom_token_server",
        args=["POST", "https://example.com"],
        kwargs={
            "data": {"user": "u", "password ": "p"},
            "filter": '"CustomScheme \(.data.toucan_token)"',  # noqa: W605
            "token_header_name": "CustomAuthorization",
        },
    )

    session = auth.get_session()
    assert isinstance(session.auth, CustomTokenServer)

    responses.add(responses.POST, "https://example.com", json={"data": {"toucan_token": "1234567"}})

    session.auth(DummyRequest())
    assert DummyRequest.headers["CustomAuthorization"] == "CustomScheme 1234567"


@responses.activate
def test_custom_token_server_initial_basic():
    """
    Custom_token_server with its own auth class for the initial request
    """
    responses.add(responses.POST, "https://example.com", json={"token": "a"})
    session = Auth(
        type="custom_token_server",
        args=["POST", "https://example.com"],
        kwargs={"auth": {"type": "basic", "args": ["u", "p"]}},
    ).get_session()

    session.auth(DummyRequest())
    assert responses.calls[0].request.headers["Authorization"].startswith("Basic")


@responses.activate
def test_oauth2_oidc():
    # Case 1: id_token is valid and not expired
    id_token = jwt.encode({"exp": (datetime.now() + timedelta(seconds=10000)).timestamp(), "user": "babar"}, key="key")
    auth = Auth(
        type="oauth2_oidc",
        args=[],
        kwargs={
            "id_token": id_token,
            "client_id": "example_client_id",
            "refresh_token": jwt.encode(
                {"exp": (datetime.now() + timedelta(seconds=100000)).timestamp()}, key="refreshkey"
            ),
            "client_secret": "example_client_secret",
            "token_endpoint": "https://example.com/token",
            "refresh": True,
        },
    )
    session = auth.get_session()
    assert session.headers["Authorization"] == f"Bearer {id_token}"

    # Case 2: id_token is expired but refresh token is not
    id_token = jwt.encode({"exp": (datetime.now() - timedelta(seconds=100)).timestamp(), "aud": "babar"}, key="key")
    auth.kwargs["id_token"] = id_token
    responses.add(
        method=responses.POST,
        url="https://example.com/token",
        json={"id_token": "coucou"},
    )
    session = auth.get_session()


def test_build_auth_kwargs_only() -> None:
    auth = Auth(kwargs={"username": "a", "password": "b"}, type="basic")
    assert auth.kwargs == {"username": "a", "password": "b"}
    assert auth.args == []
    assert auth.type == AuthType.basic
