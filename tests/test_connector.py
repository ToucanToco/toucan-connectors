import pytest

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class DataSource(ToucanDataSource):
    query: str


class DataConnector(ToucanConnector):
    type = 'MyDB'
    data_source_model: DataSource

    def get_df(self, data_source): pass


################################################
def test_missing_attributes():
    # missing type
    with pytest.raises(TypeError) as exc_info:
        class MissingDataConnector1(ToucanConnector):
            data_source_model = DataSource

            def get_df(self, data_source): pass
    assert str(exc_info.value) == "MissingDataConnector1 has no 'type' attribute."

    # missing data_source_model
    with pytest.raises(TypeError) as exc_info:
        class MissingDataConnector2(ToucanConnector):
            type = 'MyDB'

            def get_df(self, data_source): pass
    assert str(exc_info.value) == "MissingDataConnector2 has no 'data_source_model' attribute."


def test_no_get_df():
    class BadDataConnector(ToucanConnector):
        type = 'MyDB'
        data_source_model = 'asd'

    with pytest.raises(TypeError):
        BadDataConnector(name='my_name')


def test_type():
    dc = DataConnector(**{'name': 'my_name'})
    assert dc.type == 'MyDB'
    assert dc.name == 'my_name'
    assert dc.data_source_model == DataSource


def test_validate():
    dc = DataConnector(name='my_name')
    dc.data_source_model.validate({
        'query': '',
        'name': 'my_name',
        'domain': 'my_domain'
    })
