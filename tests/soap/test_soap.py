import json

import pandas as pd
from mock import Mock
from pandas._testing import assert_frame_equal
from pytest import fixture

from toucan_connectors.soap.soap_connector import SoapConnector, SoapDataSource

import_path = 'toucan_connectors.soap.soap_connector'


@fixture
def connector():
    return SoapConnector(
        name='foobar',
        headers={'foo': 'bar'},
        endpoint='https://example.com?wsdl',
    )


@fixture
def create_datasource():
    return SoapDataSource(
        name='foosource',
        domain='test_domain',
        method='fake_func',
        parameters={'arg': 'foo'},
        flatten_column='nested',
    )


def test_schema_fields_order(connector, create_datasource):
    schema_props_keys = list(json.loads(SoapDataSource.schema_json())['properties'].keys())
    assert schema_props_keys[0] == 'domain'
    assert schema_props_keys[1] == 'method'
    assert schema_props_keys[2] == 'parameters'
    assert schema_props_keys[3] == 'flatten_column'


def test__get_methods_docs(mocker, create_datasource):
    """
    Check that _get_methods_docs is able to list available methods and
    associated docstring from a given service
    """

    def fake_func():
        """coucou"""

    mocked_client = mocker.patch(f'{import_path}.Client')
    mocked_service = mocked_client.service
    mocked_service.fake_func = fake_func
    methods_docs = create_datasource._get_methods_docs(mocked_client)
    assert methods_docs['fake_func'] == 'coucou'


def test_get_form(mocker, connector, create_datasource):
    """
    Check that get_form correctly returns a data source form with prefilled informations
    """
    mocker.patch(f'{import_path}.SoapConnector.create_client')
    mocker.patch(
        f'{import_path}.SoapDataSource._get_methods_docs', return_value={'fake_func': 'coucou'}
    )
    form = create_datasource.get_form(connector, {})
    assert form['properties']['parameters']['description'] == 'Services documentation: <br> coucou'
    assert form['definitions']['method']['enum'][0] == 'fake_func'


def test_create_client(mocker, connector):
    """
    Check that create client correctly create and return a zeep client
    """
    mocker.patch(f'{import_path}.Session', return_value=Mock(headers={}))
    mocked_client = mocker.patch(f'{import_path}.Client')
    mocked_transport = mocker.patch(f'{import_path}.Transport')
    connector.create_client()
    mocked_transport.assert_called_once()
    mocked_client.assert_called_once()


def test__retrieve_data_with_flatten(mocker, connector, create_datasource):
    """
    Check that the connector is able to retrieve data from SOAP API
    """

    def fake_func(arg: str):
        """coucou"""
        return [{'id': 1, 'nested': {'bla': 'bla'}}, {'id': 2, 'nested': {'bla': 'foo'}}]

    m = Mock()
    m.service.fake_func = fake_func
    mocker.patch(f'{import_path}.SoapConnector.create_client', return_value=m)
    data = connector._retrieve_data(create_datasource)
    assert_frame_equal(
        data,
        pd.DataFrame(
            {
                'id': {0: 1, 1: 2},
                'nested.bla': {0: 'bla', 1: 'foo'},
                'nested': {0: {'bla': 'bla'}, 1: {'bla': 'foo'}},
            }
        ),
    )


def test__retrieve_data(mocker, connector):
    """
    Check that the connector is able to retrieve data from SOAP API with nested
    response
    """
    ds = SoapDataSource(
        name='foosource',
        domain='test_domain',
        method='fake_func',
        parameters={'arg': 'foo'},
    )

    def fake_func(arg: str):
        """coucou"""
        return [{'id': 1, 'bla': 'bla'}, {'id': 2, 'bla': 'foo'}]

    m = Mock()
    m.service.fake_func = fake_func
    mocker.patch(f'{import_path}.SoapConnector.create_client', return_value=m)
    data = connector._retrieve_data(ds)
    assert_frame_equal(
        data,
        pd.DataFrame({'id': {0: 1, 1: 2}, 'bla': {0: 'bla', 1: 'foo'}}),
    )


def test__retrieve_data_scalar_response(mocker, connector):
    """
    Check that the connector is able to retrieve data from SOAP API
    with scalar value
    """
    ds = SoapDataSource(
        name='foosource',
        domain='test_domain',
        method='fake_func',
        parameters={'arg': 'foo'},
    )

    def fake_func(arg: str):
        """coucou"""
        return 10

    m = Mock()
    m.service.fake_func = fake_func
    mocker.patch(f'{import_path}.SoapConnector.create_client', return_value=m)
    data = connector._retrieve_data(ds)
    assert_frame_equal(
        data,
        pd.DataFrame({'response': 10}, index=[0]),
    )
