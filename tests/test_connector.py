import pytest

from toucan_connectors.toucan_pydantic_connector import (
        ToucanConnector, ToucanDataSource
)


class DataSource(ToucanDataSource):
    query: str


class DataConnector(ToucanConnector):
    type = 'MyDB'
    data_source = DataSource

    def get_df(self, data_source): pass


def test_no_get_df():

    class DataConnector(ToucanConnector):
        type = 'MyDB'

    with pytest.raises(TypeError) as e:
        DataConnector(name='my_name')
    assert e.type == TypeError


def test_type():
    dc = DataConnector(**{'name': 'my_name'})
    assert dc.type == 'MyDB'
    assert dc.name == 'my_name'
    assert dc.data_source == DataSource


def test_validate():
    dc = DataConnector(name='my_name')
    dc.data_source.validate({
        'query': '',
        'name': 'my_name',
        'domain': 'my_domain'
    })
