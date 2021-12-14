from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError
from redshift_connector.error import InterfaceError, ProgrammingError

from toucan_connectors import DataSlice
from toucan_connectors.redshift.redshift_database_connector import (
    AuthenticationMethod,
    RedshiftConnector,
    RedshiftDataSource,
)


@pytest.fixture
def redshift_connector():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.DB_CREDENTIALS,
        name='test',
        host='localhost',
        port=0,
        user='user',
        password='sample',
        connect_timeout=10,
    )


@pytest.fixture
def redshift_connector_aws_creds():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.AWS_CREDENTIALS,
        name='test',
        host='localhost',
        port=0,
        db_user='db_user_test',
        cluster_identifier='cluster_test',
        access_key_id='access_key',
        secret_access_key='secret_access_key',
        session_token='token',
        region='eu-west-1',
    )


@pytest.fixture
def redshift_connector_aws_profile():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.AWS_PROFILE,
        name='test',
        host='localhost',
        port=0,
        db_user='db_user_test',
        cluster_identifier='cluster_test',
        region='eu-west-1',
        profile='sample',
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
            'port': 0,
            'cluster_identifier': 'cluster_identifier_test',
            'db_user': 'db_user_test',
            'connect_timeout': 'connect_timeout_test',
            'authentication_method': 'authentication_method_test',
            'user': 'user_test',
            'password': 'password_test',
            'access_key_id': 'access_key_id_test',
            'secret_access_key': 'secret_access_key_test',
            'session_token': 'session_token_test',
            'profile': 'profile_test',
            'region': 'region_test',
        }
    }
    result = RedshiftConnector.Config().schema_extra(schema)
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


def test_redshiftconnector_get_connection_params_db_cred_mode_missing_params():
    with pytest.raises(ValueError) as exc_info_user:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.DB_CREDENTIALS,
            name='test',
            host='localhost',
            port=0,
            password='pass',
        )
    assert f'User & Password are required for {AuthenticationMethod.DB_CREDENTIALS}' in str(
        exc_info_user.value
    )
    with pytest.raises(ValueError) as exc_info_pwd:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.DB_CREDENTIALS,
            name='test',
            host='localhost',
            port=0,
            user='user',
        )
    assert f'User & Password are required for {AuthenticationMethod.DB_CREDENTIALS}' in str(
        exc_info_pwd.value
    )


def test_redshiftconnector_get_connection_params_db_cred_mode(redshift_connector):
    result = redshift_connector._get_connection_params(database='test')
    assert result == dict(
        host='localhost', database='test', port=0, user='user', password='sample', timeout=10
    )


def test_redshiftconnector_get_connection_params_aws_creds_mode_missing_params():
    with pytest.raises(ValueError) as exc_info_session:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_CREDENTIALS,
            name='test',
            host='localhost',
            port=0,
            db_user='db_user_test',
            cluster_identifier='cluster_test',
            access_key_id='access_key',
            secret_access_key='secret_access_key',
            region='eu-west-1',
        )
    assert (
        f'AccessKeyId, SecretAccessKey & SessionToken are required for {AuthenticationMethod.AWS_CREDENTIALS}'
        in str(exc_info_session.value)
    )
    with pytest.raises(ValueError) as exc_info_secret:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_CREDENTIALS,
            name='test',
            host='localhost',
            port=0,
            db_user='db_user_test',
            cluster_identifier='cluster_test',
            access_key_id='access_key',
            session_token='token',
            region='eu-west-1',
        )
    assert (
        f'AccessKeyId, SecretAccessKey & SessionToken are required for {AuthenticationMethod.AWS_CREDENTIALS}'
        in str(exc_info_secret.value)
    )
    with pytest.raises(ValueError) as exc_info_key:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_CREDENTIALS,
            name='test',
            host='localhost',
            port=0,
            db_user='db_user_test',
            cluster_identifier='cluster_test',
            secret_access_key='secret_access_key',
            session_token='token',
            region='eu-west-1',
        )
    assert (
        f'AccessKeyId, SecretAccessKey & SessionToken are required for {AuthenticationMethod.AWS_CREDENTIALS}'
        in str(exc_info_key.value)
    )


def test_redshiftconnector_get_connection_params_aws_creds_mode(redshift_connector_aws_creds):
    result = redshift_connector_aws_creds._get_connection_params(database='test')
    assert result == dict(
        host='localhost',
        database='test',
        port=0,
        iam=True,
        db_user='db_user_test',
        cluster_identifier='cluster_test',
        access_key_id='access_key',
        secret_access_key='secret_access_key',
        session_token='token',
        region='eu-west-1',
    )


def test_redshiftconnector_get_connection_params_aws_profile_mode_missing_params():
    with pytest.raises(ValueError):
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_PROFILE,
            name='test',
            host='localhost',
            port=0,
            db_user='db_user_test',
            region='eu-west-1',
        )
    assert ValidationError(
        model='RedshiftConnector',
        errors=[
            {
                'loc': ('__root__',),
                'msg': 'Profile are required for aws_profile',
                'type': 'value_error',
            }
        ],
    )


def test_redshiftconnector_get_connection_params_aws_profile_mode(redshift_connector_aws_profile):
    result = redshift_connector_aws_profile._get_connection_params(database='test')
    assert result == dict(
        host='localhost',
        database='test',
        port=0,
        iam=True,
        db_user='db_user_test',
        cluster_identifier='cluster_test',
        region='eu-west-1',
        profile='sample',
    )


def test_redshiftconnector_get_connection_params_missing_authentication_mode():
    with pytest.raises(ValueError) as exc_info:
        RedshiftConnector(
            name='test',
            host='localhost',
            port=0,
        )
    assert 'Unknown AuthenticationMethod' in str(exc_info.value)


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_build_connection(
    mock_redshift_connector, redshift_connector, redshift_datasource
):
    redshift_connector.authentication_method = AuthenticationMethod.DB_CREDENTIALS
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


def test_redshiftconnector_close(redshift_connector):
    cm = redshift_connector.get_redshift_connection_manager()
    cm.force_clean()


@patch('toucan_connectors.redshift.redshift_database_connector.Thread')
def test_redshiftconnector_start_timer_alive(mock_thread, redshift_connector):
    redshift_connector._start_timer_alive()
    assert mock_thread().start.called


def test_redshiftconnector_set_alive_done(redshift_connector):
    redshift_connector.connect_timeout = 2
    redshift_connector._set_alive_done()
    assert redshift_connector._is_alive is False


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
