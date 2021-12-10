from unittest.mock import Mock, patch

import pytest

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
        port=0,
    )


@pytest.fixture
def redshift_datasource():
    return RedshiftDataSource(
        domain='test', name='redshift', database='test', query='SELECT * FROM public.sales;'
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftdatasource_get_form(
    mock_redshift_connector, redshift_connector, redshift_datasource
):
    current_config = {}
    mock_redshift_connector.connect().return_value = Mock()
    result = redshift_datasource.get_form(redshift_connector, current_config)
    assert result['properties']['parameters']['title'] == 'Parameters'
    assert result['properties']['table']['title'] == 'Table'
    assert result['properties']['validation']['title'] == 'Validation'
    assert result['required'] == ['domain', 'name', 'database', 'query']


def test_redshiftconnector_get_connection_params(redshift_connector):
    result = redshift_connector.get_connection_params(database='test')
    assert result == dict(
        host='localhost',
        database='test',
        user='user',
        port=0,
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_get_connection(
    mock_redshift_connector, redshift_connector, redshift_datasource
):
    redshift_mock = Mock()
    mock_redshift_connector.connect.return_value = redshift_mock
    result = redshift_connector._get_connection(datasource=redshift_datasource)
    assert result == redshift_mock


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_cursor(mock_get_connection, redshift_connector, redshift_datasource):
    connection_mock = Mock()
    mock_get_connection().cursor.return_value = connection_mock
    result = redshift_connector._get_cursor(datasource=redshift_datasource)
    assert result == connection_mock


@patch.object(RedshiftConnector, '_get_cursor')
def test_redshiftconnector_retrieve_data(mock_cursor, redshift_connector, redshift_datasource):
    mock = Mock()
    mock_cursor().__enter__().fetch_dataframe.return_value = mock
    result = redshift_connector._retrieve_data(datasource=redshift_datasource)
    assert result == mock


# @patch.object(RedshiftConnector, '_get_connection')
# def test_redshiftconnector_get_status(mock_connection, redshift_connector):
#     mock_connection().__enter__().return_value = Mock()
#     result = redshift_connector.get_status()
#     assert result.status is True
#     assert result.error is None
#
#
# @patch.object(RedshiftConnector, '_get_connection')
# def test_redshiftconnector_get_status_with_error(mock_connection, redshift_connector):
#     mock_connection.side_effect = InterfaceError('error mock')
#     result = redshift_connector.get_status()
#     assert type(result.error) == InterfaceError
#     assert result.status is False
#     assert str(result.error) == 'error mock'
