import pytest

from toucan_connectors.bases import ToucanConnector


def test_no_type():

    class DataConnector(ToucanConnector):
        def connect(self): pass
        def disconnect(self): pass
        def get_df(self, data_source): pass
        def validate(self, data_source): pass

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

    class DataConnector(ToucanConnector):
        type = 'MyDB'

        def connect(self): pass
        def disconnect(self): pass
        def get_df(self, data_source): pass
        def validate(self, data_source): pass

    dc = DataConnector(**{'name':'my_name'})
    assert dc.type == 'MyDB'
    assert dc.name == 'my_name'
