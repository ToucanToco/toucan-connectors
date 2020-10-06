"""
This provide a helper to test OAuth2 connectors locally
"""
import json
import webbrowser
import wsgiref.simple_server
import wsgiref.util
from os import path
from typing import Any


class _RedirectWSGIApp(object):
    """WSGI app to handle the authorization redirect.

    Stores the request URI and displays the given success message.
    """

    def __init__(self, success_message):
        """
        Args:
            success_message (str): The message to display in the web browser
                the authorization flow is complete.
        """
        self.last_request_uri = None
        self._success_message = success_message

    def __call__(self, environ, start_response):
        """WSGI Callable.

        Args:
            environ (Mapping[str, Any]): The WSGI environment.
            start_response (Callable[str, list]): The WSGI start_response
                callable.

        Returns:
            Iterable[bytes]: The response body.
        """
        start_response('200 OK', [('Content-type', 'text/plain')])
        self.last_request_uri = wsgiref.util.request_uri(environ)
        return [self._success_message.encode('utf-8')]


def get_authorization_response(authorization_url, host, port):
    """
    Open a browser to get consent from the provider UI
    Create an ephemeral webserver to catch the provider redirection
    """
    webbrowser.open(authorization_url, new=1, autoraise=True)
    app = _RedirectWSGIApp('OK')
    local_server = wsgiref.simple_server.make_server(host, port, app)
    local_server.handle_request()
    return app.last_request_uri


class JsonFileSecretsKeeper:
    def __init__(self, filename: str):
        self.filename = filename

    def load_file(self) -> dict:
        if not path.exists(self.filename):
            return {}
        with open(self.filename, 'r') as f:
            return json.load(f)

    def save(self, key: str, value):
        values = self.load_file()
        values[key] = value
        with open(self.filename, 'w') as f:
            json.dump(values, f)

    def load(self, key: str) -> Any:
        return self.load_file()[key]
