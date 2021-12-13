from unittest.mock import Mock, patch

import pytest
from redshift_connector.error import InterfaceError, ProgrammingError

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


def test_redshiftdatasource_init():
    ds = RedshiftDataSource(domain='test', name='redshift', database='test', table='table_test')
    assert ds.query == 'select * from table_test;'
    assert ds.table == 'table_test'

    with pytest.raises(ValueError):
        ds = RedshiftDataSource(domain='test', name='redshift', database='test')
        assert ds.query is None
        assert ds.table is None


@patch.object(RedshiftConnector, '_retrieve_tables')
def test_redshiftdatasource_get_form(redshift_connector, redshift_datasource):
    current_config = {'database': ['table1', 'table2', 'table3']}
    redshift_connector._retrieve_tables.return_value = ['table1', 'table2', 'table3']
    result = redshift_datasource.get_form(redshift_connector, current_config)
    assert result['properties']['parameters']['title'] == 'Parameters'
    assert result['properties']['domain']['title'] == 'Domain'
    assert result['properties']['validation']['title'] == 'Validation'
    assert result['required'] == ['domain', 'name', 'database']


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connection_manager')
def test_redshiftconnector_get_redshift_connection_manager(
    mock_connection_manager, redshift_connector
):
    assert redshift_connector.get_redshift_connection_manager() == mock_connection_manager


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


# @patch.object(RedshiftConnector, '_get_connection')
# def test_redshiftconnector_get_connection_alive_function( redshift_connector,
# ):
#     redshift_connector.connect_timeout = 3
#     result = redshift_connector.alive_function(Mock())
#     assert result is True


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_cursor(mock_get_connection, redshift_connector, redshift_datasource):
    connection_mock = Mock()
    mock_get_connection().cursor.return_value = connection_mock
    result = redshift_connector._get_cursor(datasource=redshift_datasource)
    assert result == connection_mock


@patch.object(RedshiftConnector, '_get_cursor')
def test_redshiftconnector_retrieve_tables(mock_cursor, redshift_connector, redshift_datasource):
    mock_cursor().__enter__().fetchall.return_value = (['table1'], ['table2'], ['table3'])
    result = redshift_connector._retrieve_tables(datasource=redshift_datasource)
    assert result == ['table1', 'table2', 'table3']


@patch.object(RedshiftConnector, '_get_cursor')
def test_redshiftconnector_retrieve_data(mock_cursor, redshift_connector, redshift_datasource):
    mock = Mock()
    mock_cursor().__enter__().fetch_dataframe.return_value = mock
    result = redshift_connector._retrieve_data(datasource=redshift_datasource)
    assert result == mock


def test_redshiftconnector__get_details(redshift_connector):
    result = redshift_connector._get_details(index=0, status=True)
    assert result == [('Hostname resolved', True), ('Port opened', False), ('Authenticated', False)]


@patch.object(RedshiftConnector, '_build_connection')
@patch.object(RedshiftConnector, 'check_hostname')
@patch.object(RedshiftConnector, 'check_port')
def test_redshiftconnector_get_status_true(
    mock_check_hostname, mock_check_port, mock_build_connection, redshift_connector
):
    mock_check_hostname.return_value = 'hostname_test'
    mock_check_port.return_value = 'port_test'
    mock_build_connection().__enter__().return_value = True
    result = redshift_connector.get_status()
    assert result.status is True
    assert result.error is None


@patch.object(RedshiftConnector, 'check_hostname')
def test_redshiftconnector_get_status_with_error_host(mock_hostname, redshift_connector):
    mock_hostname.side_effect = InterfaceError('error mock')
    result = redshift_connector.get_status()
    assert type(result.error) == str
    assert result.status is False
    assert str(result.error) == 'error mock'


@patch.object(RedshiftConnector, 'check_port')
def test_redshiftconnector_get_status_with_error_port(mock_hostname, redshift_connector):
    mock_hostname.side_effect = InterfaceError('error mock')
    result = redshift_connector.get_status()
    assert type(result.error) == str
    assert result.status is False
    assert str(result.error) == 'error mock'


@patch.object(RedshiftConnector, '_build_connection')
@patch.object(RedshiftConnector, 'check_hostname')
@patch.object(RedshiftConnector, 'check_port')
def test_redshiftconnector_get_status_programming_error(
    mock_check_hostname, mock_check_port, mock_build_connection, redshift_connector
):
    mock_check_hostname.return_value = 'hostname_test'
    mock_check_port.return_value = 'port_test'
    redshift_connector.user = 'user_test'
    mock_build_connection.side_effect = ProgrammingError(
        "'S': 'FATAL', 'C': '3D000', 'M': 'database user_test does not exist'"
    )
    result = redshift_connector.get_status()
    assert result.status is True
    assert result.error == "'S': 'FATAL', 'C': '3D000', 'M': 'database user_test does not exist'"


@patch.object(RedshiftConnector, '_build_connection')
@patch.object(RedshiftConnector, 'check_hostname')
@patch.object(RedshiftConnector, 'check_port')
def test_redshiftconnector_get_status_exception(
    mock_check_hostname, mock_check_port, mock_build_connection, redshift_connector
):
    mock_check_hostname.return_value = 'hostname_test'
    mock_check_port.return_value = 'port_test'
    redshift_connector.user = 'user_test'
    mock_build_connection.side_effect = Exception
    result = redshift_connector.get_status()
    print(result)
    assert result.status is True
    assert result.error is None
