import pytest

from connectors.abstract_connector import (
    AbstractConnector,
    BadSignature,
    BadParameters
)


def test_keywords_only():
    class BadConnector(AbstractConnector):
        def __init__(self, host, *, server=None):
            self.host = host
            self.server = server

        def connect(self): pass

        def disconnect(self): pass

        def run_query(self, query): pass

        def get_df(self, config): pass

    with pytest.raises(BadSignature) as exc_info:
        BadConnector('zbruh')
    assert str(exc_info.value) == 'All parameters must be keywords only (self, *, host, ...)'


def test_kwargs_forbidden():
    class BadConnector(AbstractConnector):
        def __init__(self, *, host, **kwargs):
            self.host = host

        def connect(self): pass

        def disconnect(self): pass

        def run_query(self, query): pass

        def get_df(self, config): pass

    with pytest.raises(BadSignature) as exc_info:
        BadConnector(name='zbruh', host='localhost')
    assert str(exc_info.value) == 'All parameters must be explicitly named (**kwargs forbidden)'


# def test_mandatory_args():
#     class BadConnector(AbstractConnector):
#         def __init__(self, *, host):
#             self.host = host
#
#         def connect(self): pass
#
#         def disconnect(self): pass
#
#         def run_query(self, query): pass
#
#         def get_df(self, config): pass
#
#     with pytest.raises(MissingConnectorName) as exc_info:
#         BadConnector(host='localhost')
#     assert str(exc_info.value) == '"name" is a mandatory parameter'


def test_bad_parameters():
    class GoodConnector(AbstractConnector):
        def __init__(self, *, name, host, server=None, port=None):
            self.name = name
            self.host = host
            self.server = server
            self.port = port

        def connect(self): pass

        def disconnect(self): pass

        def run_query(self, query): pass

        def get_df(self, config): pass

    # It should not fail when trying to instanciate these connectors
    GoodConnector(name='ok', host='localhost')
    GoodConnector(name='ok', host='localhost', port=8080)
    GoodConnector(name='ok', host='localhost', port=8080, server='localserver')

    with pytest.raises(BadParameters) as exc_info:
        GoodConnector(name='ok')
    assert str(exc_info.value) == "Missing parameters for GoodConnector " \
                                  "(mandary parameters: ['name', 'host'], " \
                                  "optional: {'server': None, 'port': None})"

    with pytest.raises(BadParameters) as exc_info:
        GoodConnector(name='ok', host='localhost', extra='toomuch', port=8080)
    assert str(exc_info.value) == "Too many parameters for GoodConnector " \
                                  "(mandary parameters: ['name', 'host'], " \
                                  "optional: {'server': None, 'port': None})"
