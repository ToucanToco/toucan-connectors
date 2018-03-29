import pytest

from toucan_connectors.bases import (
        ToucanConnector, ToucanDataSource, BadParameters
)


class DataSource(ToucanDataSource):
    query: str


class DataConnector(ToucanConnector):
    type = 'MyDB'
    data_source_class = DataSource

    def connect(self): pass

    def disconnect(self): pass

    def get_df(self, data_source): pass


def test_no_type():

    class DataConnector(ToucanConnector):
        def connect(self): pass

        def disconnect(self): pass

        def get_df(self, data_source): pass

    with pytest.raises(TypeError) as e:
        DataConnector()
    assert e.type == TypeError


def test_no_get_df():

    class DataConnector(ToucanConnector):
        type = 'MyDB'

        def connect(self): pass

        def disconnect(self): pass

    with pytest.raises(TypeError) as e:
        DataConnector(name='my_name')
    assert e.type == TypeError


def test_type():
    dc = DataConnector(**{'name': 'my_name'})
    assert dc.type == 'MyDB'
    assert dc.name == 'my_name'


def test_validate():
    DataConnector.validate({
        'query': '',
        'name': 'my_name',
        'domain': 'my_domain'
    })


def test_invalidate():
    with pytest.raises(BadParameters) as e:
        DataConnector.validate({'query': ''})
    assert e.type == BadParameters


def test_validate_no_class():

    class DataConnector(ToucanConnector):
        type = 'MyDB'

        def connect(self): pass

        def disconnect(self): pass

        def get_df(self, data_source): pass

    with pytest.raises(TypeError) as e:
        DataConnector.validate({'query': ''})
    assert e.type == TypeError
