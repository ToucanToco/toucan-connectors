from time import time

import pandas as pd
import pytest
import tenacity as tny
from pydantic import create_model

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.google_sheets_2.google_sheets_2_connector import GoogleSheets2Connector
from toucan_connectors.mongo.mongo_connector import MongoConnector
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2ConnectorConfig
from toucan_connectors.toucan_connector import (
    ToucanConnector,
    ToucanDataSource,
    get_connector_secrets_form,
    strlist_to_enum,
)


class DataSource(ToucanDataSource):
    query: str


class DataConnector(ToucanConnector):
    type = 'MyDB'
    data_source_model: DataSource

    def _retrieve_data(self, data_source):
        pass


################################################
def test_missing_attributes():
    # missing data_source_model
    with pytest.raises(TypeError) as exc_info:

        class MissingDataConnector2(ToucanConnector):
            type = 'MyDB'

            def _retrieve_data(self, data_source):
                pass

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
    dc.data_source_model.validate({'query': '', 'name': 'my_name', 'domain': 'my_domain'})


def test_get_df_with_permissions():
    class DataConnector(ToucanConnector):
        type = 'MyDB'
        data_source_model: DataSource

        def _retrieve_data(self, datasource):
            return pd.DataFrame({'A': [1, 2]})

    connector = DataConnector(name='my_name')
    ds = connector.data_source_model(domain='yo', name='my_name', query='')
    df = connector.get_df(ds, permissions={'column': 'A', 'operator': 'eq', 'value': 1})
    assert all(df == pd.DataFrame({'A': [1]}))


def test_get_slice():
    class DataConnector(ToucanConnector):
        type = 'MyDB'
        data_source_model = 'asd'

        def _retrieve_data(self, datasource):
            return pd.DataFrame({'A': [1, 2, 3, 4, 5]})

    # without offset without limit
    res = DataConnector(name='my_name').get_slice({})
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({'A': [1, 2, 3, 4, 5]}))

    # without offset with limit
    res = DataConnector(name='my_name').get_slice({}, limit=1)
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({'A': [1]}))

    # with offset without limit
    res = DataConnector(name='my_name').get_slice({}, offset=2)
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({'A': [3, 4, 5]}))
    assert res.stats.total_returned_rows == 5

    # with offset with limit
    res = DataConnector(name='my_name').get_slice({}, offset=2, limit=2)
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({'A': [3, 4]}))
    assert res.stats.total_returned_rows == 5


def test_explain():
    class DataConnector(ToucanConnector):
        type = 'MyDB'
        data_source_model = 'asd'

        def _retrieve_data(self, datasource):
            return pd.DataFrame()

    res = DataConnector(name='my_name').explain({})
    assert res is None


def test_get_status():
    assert DataConnector(name='my_name').get_status() == ConnectorStatus()


def test_get_cache_key():
    connector = DataConnector(name='my_name')
    ds = connector.data_source_model(domain='yo', name='my_name', query='much caching')

    key = connector.get_cache_key(ds)
    # We should get a deterministic identifier:
    # /!\ the identifier will change if the model of the connector or the datasource changes
    assert key == '1d5ac63f-fcb1-32cc-b015-5293887ca675'

    ds.query = 'wow'
    key2 = connector.get_cache_key(ds)
    assert key2 != key

    ds.query = 'much caching'
    key3 = connector.get_cache_key(ds)
    assert key3 == key


def test_get_cache_key_connector_alone():
    connector_a1 = DataConnector(name='a')
    connector_a2 = DataConnector(name='a')
    connector_b = DataConnector(name='b')

    key_a1 = connector_a1.get_cache_key()
    key_a2 = connector_a2.get_cache_key()
    key_b = connector_b.get_cache_key()

    assert key_a1 == key_a2
    assert key_a1 != key_b


class UnreliableDataConnector(ToucanConnector):
    type = 'MyUnreliableDB'
    data_source_model: DataSource

    def _retrieve_data(self, data_source, logbook=[]):
        if len(logbook) < 3:
            logbook.append(time())
            raise RuntimeError('try again!')
        logbook.clear()
        return 42


@pytest.mark.skip(reason='Connectors tests currently fail on GitHub CI, for an unknown reason')
def test_max_attempt_df():
    udc = UnreliableDataConnector(name='my_name', retry_policy={'max_attempts': 5})
    result = udc.get_df({})
    assert result == 42


class CustomPolicyDataConnector(ToucanConnector):
    type = 'MyUnreliableDB'
    data_source_model: DataSource

    def _retrieve_data(self, data_source, logbook=[]):

        if len(logbook) < 3:
            logbook.append(time())
            raise RuntimeError('try again!')
        logbook.clear()
        return pd.DataFrame({'1': [42, 32]})

    @property
    def retry_decorator(self):
        return tny.retry(stop=tny.stop_after_attempt(5))


def test_custom_max_attempt_df():
    udc = CustomPolicyDataConnector(name='my_name')
    result = udc.get_df({})
    assert result['1'].values.tolist() == [42, 32]


class CustomRetryOnDataConnector(ToucanConnector):
    type = 'MyUnreliableDB'
    data_source_model: DataSource
    _retry_on = (ValueError,)

    def _retrieve_data(self, data_source, logbook=[]):
        if len(logbook) < 3:
            logbook.append(time())
            raise RuntimeError('try again!')
        logbook.clear()
        return 42


def test_custom_retry_on_df():
    udc = CustomRetryOnDataConnector(name='my_name')
    with pytest.raises(RuntimeError):
        udc.get_df({})


class CustomNoRetryOnDataConnector(ToucanConnector):
    type = 'MyUnreliableDB'
    data_source_model: DataSource

    @property
    def retry_decorator(self):
        return None

    def _retrieve_data(self, data_source, logbook=[]):
        if len(logbook) == 0:
            logbook.append(time())
            raise RuntimeError('try again!')
        logbook.clear()
        return 42


def test_no_retry_on_df():
    udc = CustomNoRetryOnDataConnector(name='my_name')
    with pytest.raises(RuntimeError):
        udc.get_df({})


def test_strlist_to_enum_required():
    """It should be required by default"""
    model = create_model('Test', pokemon=strlist_to_enum('pokemon', ['pika', 'bulbi']))
    assert model.schema() == {
        'title': 'Test',
        'type': 'object',
        'definitions': {
            'pokemon': {
                'description': 'An enumeration.',
                'enum': ['pika', 'bulbi'],
                'title': 'pokemon',
                'type': 'string',
            }
        },
        'properties': {'pokemon': {'$ref': '#/definitions/pokemon'}},
        'required': ['pokemon'],
    }


def test_strlist_to_enum_default_value():
    """It should be possible to add a default value (not required)"""
    model = create_model('Test', pokemon=strlist_to_enum('pokemon', ['pika', 'bulbi'], 'pika'))
    assert model.schema() == {
        'title': 'Test',
        'type': 'object',
        'definitions': {
            'pokemon': {
                'description': 'An enumeration.',
                'enum': ['pika', 'bulbi'],
                'title': 'pokemon',
                'type': 'string',
            }
        },
        'properties': {
            'pokemon': {'allOf': [{'$ref': '#/definitions/pokemon'}], 'default': 'pika'}
        },
    }


def test_should_return_connector_config_form():
    assert (
        get_connector_secrets_form(GoogleSheets2Connector).secrets_schema
        == OAuth2ConnectorConfig.schema()
    )
    assert get_connector_secrets_form(MongoConnector) is None


def test_get_df_int_column(mocker):
    """The int column should be casted as str"""

    class DataConnector(ToucanConnector):
        type = 'MyDB'
        data_source_model: DataSource

        def _retrieve_data(self, datasource):
            return pd.DataFrame({0: [1, 2]})

    dc = DataConnector(name='bla')
    assert dc.get_df(mocker.MagicMock()).columns == ['0']
