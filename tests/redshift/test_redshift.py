from unittest.mock import Mock, patch

import pytest
from redshift_connector.error import InterfaceError

from toucan_connectors.redshift.redshift_database_connector import (
    RedshiftConnector,
    RedshiftDataSource,
)


@pytest.fixture
def redshift_connector():
    return RedshiftConnector(
        name='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftdatasource_get_form(mock_redshift_connector, redshift_connector):
    instance = RedshiftDataSource(database='test', domain='test', name='test')
    current_config = {}
    mock_redshift_connector.connect().return_value = Mock()
    result = instance.get_form(redshift_connector, current_config)
    assert result['properties']['parameters']['title'] == 'Parameters'
    assert result['properties']['table']['title'] == 'Table'
    assert result['properties']['validation']['title'] == 'Validation'
    assert result['required'] == ['domain', 'name', 'database']


def test_redshiftconnector_get_connection_params():
    instance = RedshiftConnector(
        name='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    result = instance.get_connection_params(database=None)
    assert result == dict(
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_get_connection(mock_redshift_connector):
    instance = RedshiftConnector(
        name='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    ds = RedshiftDataSource(
        domain='test', name='redshift', database='dev', query='SELECT * FROM public.sales;'
    )

    redshift_mock = Mock()
    mock_redshift_connector.connect.return_value = redshift_mock
    result = instance._get_connection(datasource=ds)
    assert result == redshift_mock


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_cursor(mock_get_connection):
    instance = RedshiftConnector(
        name='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    ds = RedshiftDataSource(
        domain='test', name='redshift', database='dev', query='SELECT * FROM public.sales;'
    )
    connection_mock = Mock()
    mock_get_connection().cursor.return_value = connection_mock
    result = instance._get_cursor(datasource=ds)
    assert result == connection_mock


@patch.object(RedshiftConnector, '_get_cursor')
def test_redshiftconnector_retrieve_data(mock_cursor):
    instance = RedshiftConnector(
        name='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    ds = RedshiftDataSource(
        domain='test', name='redshift', database='dev', query='SELECT * FROM public.sales;'
    )
    mock = Mock()
    mock_cursor().__enter__().fetch_dataframe.return_value = mock
    result = instance._retrieve_data(datasource=ds)
    assert result == mock


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_status(mock_connection):
    instance = RedshiftConnector(
        name='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    mock_connection().__enter__().return_value = Mock()
    result = instance.get_status()
    assert result.status is True
    assert result.error is None


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_status_with_error(mock_connection):
    instance = RedshiftConnector(
        name='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    mock_connection.side_effect = InterfaceError('error mock')
    result = instance.get_status()
    assert type(result.error) == InterfaceError
    assert result.status is False
    assert str(result.error) == 'error mock'
