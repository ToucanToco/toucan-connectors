from unittest.mock import Mock, patch

import pytest
from redshift_connector.error import InterfaceError, ProgrammingError

from toucan_connectors import DataSlice
from toucan_connectors.redshift.redshift_database_connector import (
    Config,
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


def test_config_schema_extra():
    schema = {
        'properties': {
            'type': 'type_test',
            'name': 'name_test',
            'host': 'host_test',
            'port': 'port_test',
            'authentication_method': 'authentication_method_test',
            'user': 'user_test',
            'password': 'password_test',
            'timeout': 5,
        }
    }
    result = Config().schema_extra(schema)
    assert result is None


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
    redshift_connector.authentication_method = 'db_cred'
    result = redshift_connector._get_connection_params(database='test')
    assert result == dict(
        host='localhost',
        database='test',
        user='user',
        port=0,
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_build_connection(
    mock_redshift_connector, redshift_connector, redshift_datasource
):
    redshift_connector.authentication_method = 'db_cred'
    result = redshift_connector._build_connection(datasource=redshift_datasource)
    assert result == mock_redshift_connector.connect()


@patch.object(RedshiftConnector, '_start_timer_alive')
@patch.object(RedshiftConnector, '_build_connection')
def test_redshiftconnector_get_connection(
    mock_build_connection, mock_start_timer_alive, redshift_connector, redshift_datasource
):
    redshift_connector.connect_timeout = 1
    result = redshift_connector._get_connection(datasource=redshift_datasource)
    assert result == mock_build_connection()


# @patch.object(RedshiftConnector, '_start_timer_alive')
# @patch.object(RedshiftConnector, '_build_connection')
# def test_redshiftconnector_get_connection_alive_close(
#     mock_build_connection, mock_start_timer_alive, redshift_connector, redshift_datasource
# ):
#     redshift_connector.connect_timeout = None
#     result = redshift_connector._get_connection(datasource=redshift_datasource)
#     assert result == mock_build_connection()
#     assert False


@patch('toucan_connectors.redshift.redshift_database_connector.Thread')
def test_redshiftconnector_start_timer_alive(mock_thread, redshift_connector):
    redshift_connector._start_timer_alive()
    assert mock_thread().start.called


def test_redshiftconnector_set_alive_done(redshift_connector):
    redshift_connector.connect_timeout = 2
    redshift_connector._set_alive_done()
    assert redshift_connector.is_alive is False


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


@patch.object(RedshiftConnector, '_retrieve_data')
def test_redshiftconnector_get_slice(mock_retreive_data, redshift_datasource, redshift_connector):
    result1 = redshift_connector.get_slice(data_source=redshift_datasource, offset=0, limit=3)
    assert result1 == DataSlice(df=mock_retreive_data().__getitem__(), total_count=0)
    result2 = redshift_connector.get_slice(data_source=redshift_datasource, offset=0, limit=None)
    assert result2 == DataSlice(df=mock_retreive_data().__getitem__(), total_count=0)


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
    assert result.status is True
    assert result.error == ''
