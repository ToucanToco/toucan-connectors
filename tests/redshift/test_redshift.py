from contextlib import _GeneratorContextManager
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from redshift_connector.error import InterfaceError

from toucan_connectors.redshift.redshift_database_connector import (
    ORDERED_KEYS,
    AuthenticationMethod,
    AuthenticationMethodError,
    RedshiftConnector,
    RedshiftDataSource,
)
from toucan_connectors.toucan_connector import DataSlice, DataStats

CLUSTER_IDENTIFIER: str = 'toucan_test'
DATABASE_NAME: str = 'toucan'


@pytest.fixture
def redshift_connector():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
        name='test',
        host='localhost',
        port=0,
        cluster_identifier=CLUSTER_IDENTIFIER,
        user='user',
        password='sample',
        default_database='dev',
        connect_timeout=10,
    )


@pytest.fixture
def redshift_connector_aws_creds():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
        name='test',
        host='localhost',
        port=0,
        db_user='db_user_test',
        cluster_identifier=CLUSTER_IDENTIFIER,
        access_key_id='access_key',
        secret_access_key='secret_access_key',
        session_token='token',
        default_database='dev',
        region='eu-west-1',
    )


@pytest.fixture
def redshift_connector_aws_profile():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.AWS_PROFILE.value,
        name='test',
        host='localhost',
        port=0,
        db_user='db_user_test',
        cluster_identifier=CLUSTER_IDENTIFIER,
        profile='sample',
        default_database='dev',
        region='eu-west-1',
    )


@pytest.fixture
def redshift_datasource():
    return RedshiftDataSource(
        domain='test',
        name='redshift',
        database=DATABASE_NAME,
        query='SELECT * FROM public.sales;',
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
    RedshiftConnector.Config().schema_extra(schema)
    assert schema['properties'] is not None
    keys = list(schema['properties'].keys())
    for i in range(len(keys)):
        assert keys[i] == ORDERED_KEYS[i]


def test_redshiftdatasource_init_(redshift_datasource):
    ds = RedshiftDataSource(domain='test', name='redshift', database='test')
    assert ds.language == 'sql'
    assert hasattr(ds, 'query_object')


@patch.object(RedshiftConnector, '_retrieve_tables')
def test_redshiftdatasource_get_form(redshift_connector, redshift_datasource):
    current_config = {'database': 'dev'}
    redshift_connector._retrieve_tables.return_value = ['table1', 'table2', 'table3']
    result = redshift_datasource.get_form(redshift_connector, current_config)
    assert result['properties']['parameters']['title'] == 'Parameters'
    assert result['properties']['domain']['title'] == 'Domain'
    assert result['properties']['validation']['title'] == 'Validation'
    assert result['required'] == ['domain', 'name']


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connection_manager')
def test_redshiftconnector_get_redshift_connection_manager(
    mock_connection_manager, redshift_connector
):
    assert redshift_connector.get_redshift_connection_manager() == mock_connection_manager


def test_redshiftconnector_get_connection_params_missing_authentication_mode():
    with pytest.raises(ValueError) as exc_info_user:
        RedshiftConnector(
            name='test',
            host='localhost',
            cluster_identifier='sample',
            port=0,
        )
    assert AuthenticationMethodError.UNKNOWN.value in str(exc_info_user.value)


def test_redshiftconnector_get_connection_params_db_cred_mode_missing_params():
    with pytest.raises(ValueError) as exc_info_user:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
            name='test',
            cluster_identifier='sample',
            host='localhost',
            port=0,
            password='pass',
        )
    assert AuthenticationMethodError.DB_CREDENTIALS.value in str(exc_info_user.value)

    # TODO: Partial check due to missing context in some operations (Missing: password)
    # with pytest.raises(ValueError) as exc_info_pwd:
    #     RedshiftConnector(
    #         authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
    #         name='test',
    #         cluster_identifier='sample',
    #         host='localhost',
    #         port=0,
    #         user='user',
    #     )
    # assert AuthenticationMethodError.DB_CREDENTIALS.value in str(exc_info_pwd.value)


def test_redshiftconnector_get_connection_params_db_cred_mode(redshift_connector):
    result = redshift_connector._get_connection_params(database='test')
    assert result == dict(
        host='localhost',
        database='test',
        cluster_identifier='toucan_test',
        port=0,
        timeout=10,
        user='user',
        password='sample',
    )


def test_redshiftconnector_get_connection_params_aws_creds_mode_missing_params():
    # TODO: Partial check due to missing context in some operations (Missing: secret_access_key)
    # with pytest.raises(ValueError) as exc_info_secret:
    #     RedshiftConnector(
    #         authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
    #         name='test',
    #         cluster_identifier='sample',
    #         host='localhost',
    #         port=0,
    #         db_user='db_user_test',
    #         access_key_id='access_key',
    #         session_token='token',
    #         region='eu-west-1',
    #     )
    # assert AuthenticationMethodError.AWS_CREDENTIALS.value in str(exc_info_secret.value)
    with pytest.raises(ValueError) as exc_info_key:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
            name='test',
            cluster_identifier='sample',
            host='localhost',
            port=0,
            db_user='db_user_test',
            secret_access_key='secret_access_key',
            session_token='token',
            region='eu-west-1',
        )
    assert AuthenticationMethodError.AWS_CREDENTIALS in str(exc_info_key.value)
    with pytest.raises(ValueError) as exc_info_db_user:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
            name='test',
            cluster_identifier='sample',
            host='localhost',
            port=0,
            access_key_id='access_key',
            secret_access_key='secret_access_key',
            session_token='token',
            region='eu-west-1',
        )
    assert AuthenticationMethodError.AWS_CREDENTIALS.value in str(exc_info_db_user.value)


def test_redshiftconnector_get_connection_params_aws_creds_mode(redshift_connector_aws_creds):
    result = redshift_connector_aws_creds._get_connection_params(database='test')
    assert result == dict(
        host='localhost',
        database='test',
        port=0,
        iam=True,
        db_user='db_user_test',
        cluster_identifier='toucan_test',
        access_key_id='access_key',
        secret_access_key='secret_access_key',
        session_token='token',
        region='eu-west-1',
    )


def test_redshiftconnector_get_connection_params_aws_profile_mode_missing_params():
    with pytest.raises(ValueError) as exc_info_profile:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_PROFILE.value,
            name='test',
            cluster_identifier='toucan_test',
            host='localhost',
            port=0,
            db_user='db_user_test',
            region='eu-west-1',
        )
    assert AuthenticationMethodError.AWS_PROFILE.value in str(exc_info_profile.value)

    with pytest.raises(ValueError) as exc_info_db_user:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_PROFILE.value,
            name='test',
            cluster_identifier='sample',
            host='localhost',
            port=0,
            profile='profile',
            region='eu-west-1',
        )
    assert AuthenticationMethodError.AWS_PROFILE.value in str(exc_info_db_user.value)


def test_redshiftconnector_get_connection_params_aws_profile_mode(redshift_connector_aws_profile):
    result = redshift_connector_aws_profile._get_connection_params(database='test')
    assert result == dict(
        host='localhost',
        database='test',
        port=0,
        iam=True,
        db_user='db_user_test',
        cluster_identifier='toucan_test',
        region='eu-west-1',
        profile='sample',
    )


@patch('toucan_connectors.ToucanConnector.get_identifier')
@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connection_manager')
def test_redshiftconnector_get_connection(
    mock_connection_manager, mock_get_identifier, redshift_connector, redshift_datasource
):
    mock_get_identifier.return_value = 'id_test'
    redshift_connector.connect_timeout = 1
    result = redshift_connector._get_connection(database=redshift_datasource)
    assert result == mock_connection_manager.get()


@patch('toucan_connectors.ToucanConnector.get_identifier')
@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_get_connection_alive_close(
    mock_get_identifier, mock_redshift_connector, redshift_connector, redshift_datasource
):
    redshift_connector._is_alive = False
    mock_redshift_connector.return_value = 'id_test'

    redshift_connector.connect_timeout = 1
    result = redshift_connector._get_connection(database=redshift_datasource)
    assert isinstance(result, _GeneratorContextManager)


@pytest.mark.skip(reason='flaky test')
@patch('toucan_connectors.ToucanConnector.get_identifier')
def test_redshiftconnector_close(mock_get_identifier, redshift_connector):
    mock_get_identifier().return_value = 'id_test'
    cm = redshift_connector.get_redshift_connection_manager()
    assert cm.connection_list[f'id_test{DATABASE_NAME}{CLUSTER_IDENTIFIER}'].exec_alive() is False
    cm.force_clean()
    assert len(cm.connection_list) == 0


def test_redshiftconnector_sleeper(redshift_connector):
    redshift_connector.connect_timeout = 0.01
    redshift_connector.sleeper()
    assert redshift_connector._is_alive is False


def test_redshiftconnector_get_cursor(redshift_connector, redshift_datasource):
    result = redshift_connector._get_cursor(database=redshift_datasource.database)
    assert isinstance(result, _GeneratorContextManager)


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_retrieve_tables(
    mock_connection, redshift_connector, redshift_datasource
):
    mock_connection().__enter__().cursor().__enter__().fetchall.return_value = (
        ['table1'],
        ['table2'],
        ['table3'],
    )
    result = redshift_connector._retrieve_tables(database=redshift_datasource.database)
    assert result == ['table1', 'table2', 'table3']


@patch.object(RedshiftConnector, '_get_connection')
@patch('toucan_connectors.redshift.redshift_database_connector.SqlQueryHelper')
def test_redshiftconnector_retrieve_data(
    mock_SqlQueryHelper, mock_get_connection, redshift_connector, redshift_datasource
):
    mock_response = Mock()
    mock_SqlQueryHelper.count_query_needed.return_value = True
    mock_SqlQueryHelper.prepare_limit_query.return_value = Mock(), Mock()
    mock_SqlQueryHelper.prepare_count_query.return_value = Mock(), Mock()
    mock_get_connection().__enter__().cursor().__enter__().fetch_dataframe.return_value = (
        mock_response
    )
    result = redshift_connector._retrieve_data(datasource=redshift_datasource, get_row_count=True)
    assert result == mock_response


@patch.object(RedshiftConnector, '_get_connection')
@patch('toucan_connectors.redshift.redshift_database_connector.SqlQueryHelper')
def test_redshiftconnector_retrieve_data_empty_result(
    mock_SqlQueryHelper, mock_get_connection, redshift_connector, redshift_datasource
):
    mock_SqlQueryHelper.count_query_needed.return_value = True
    mock_SqlQueryHelper.prepare_limit_query.return_value = Mock(), Mock()
    mock_SqlQueryHelper.prepare_count_query.return_value = Mock(), Mock()
    mock_get_connection().__enter__().cursor().__enter__().fetch_dataframe.return_value = None
    result = redshift_connector._retrieve_data(datasource=redshift_datasource, get_row_count=True)
    assert result.empty is True


@patch.object(RedshiftConnector, '_get_connection')
@patch('toucan_connectors.redshift.redshift_database_connector.SqlQueryHelper')
def test_redshiftconnector_retrieve_data_without_count(
    mock_SqlQueryHelper, mock_get_connection, redshift_connector, redshift_datasource
):
    mock_response = Mock()
    mock_SqlQueryHelper.prepare_limit_query.return_value = Mock(), Mock()
    mock_get_connection().__enter__().cursor().__enter__().fetch_dataframe.return_value = (
        mock_response
    )
    result = redshift_connector._retrieve_data(datasource=redshift_datasource, limit=10)
    assert result == mock_response


@patch.object(RedshiftConnector, '_retrieve_data')
def test_redshiftconnector_get_slice(mock_retreive_data, redshift_datasource, redshift_connector):
    mock_df = Mock()
    mock_df.__len__ = lambda x: 1
    type(mock_df).total_rows = [10]

    mock_retreive_data.return_value = mock_df
    result = redshift_connector.get_slice(
        data_source=redshift_datasource, permissions=None, offset=0, limit=1, get_row_count=True
    )
    assert result == DataSlice(
        df=mock_df, total_count=None, stats=DataStats(total_rows=10, total_returned_rows=1)
    )


@patch.object(RedshiftConnector, '_retrieve_data')
def test_redshiftconnector_get_slice_without_count(
    mock_retreive_data, redshift_datasource, redshift_connector
):
    mock_df = Mock()
    mock_df.__len__ = lambda x: 10

    mock_retreive_data.return_value = mock_df
    result = redshift_connector.get_slice(data_source=redshift_datasource)
    assert result == DataSlice(
        df=mock_df, total_count=None, stats=DataStats(total_rows=10, total_returned_rows=10)
    )


@patch.object(RedshiftConnector, '_retrieve_data')
def test_redshiftconnector_get_slice_df_is_none(
    mock_retreive_data, redshift_datasource, redshift_connector
):
    mock_retreive_data.return_value = None
    result = redshift_connector.get_slice(data_source=redshift_datasource)
    assert result == DataSlice(
        df=None, total_count=None, stats=DataStats(total_rows=0, total_returned_rows=0)
    )


def test_redshiftconnector__get_details(redshift_connector):
    result = redshift_connector._get_details(index=0, status=True)
    assert result == [
        ('Hostname resolved', True),
        ('Port opened', False),
        ('Authenticated', False),
        ('Default Database connection', False),
    ]


@patch.object(RedshiftConnector, 'check_hostname')
@patch.object(RedshiftConnector, 'check_port')
@patch.object(RedshiftConnector, '_retrieve_tables')
def test_redshiftconnector_get_status_true(
    mock_check_hostname, mock_check_port, mock_retreive_data, redshift_connector
):
    mock_check_hostname.return_value = 'hostname_test'
    mock_check_port.return_value = 'port_test'
    mock_retreive_data.return_value = ['something']
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
def test_redshiftconnector_get_status_with_error_port(mock_port, redshift_connector):
    mock_port.side_effect = InterfaceError('error mock')
    result = redshift_connector.get_status()
    assert type(result.error) == str
    assert result.status is False
    assert str(result.error) == 'error mock'


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_describe(mock_connection, redshift_connector, redshift_datasource):
    mock_description = Mock()
    type(mock_description).description = [
        (b'salesid', 23, None, None, None),
        (b'listid', 23, None, None, None),
        (b'pricepaid', 1700, None, None, None),
    ]
    mock_connection().__enter__().cursor().__enter__.return_value = mock_description
    result = redshift_connector.describe(data_source=redshift_datasource)
    expected = {'salesid': 'INTEGER', 'listid': 'INTEGER', 'pricepaid': 'DECIMAL'}
    assert result == expected


def test_get_model(mocker, redshift_connector):
    mock_get_connection = mocker.patch.object(RedshiftConnector, '_get_connection')
    mock_cols_response = pd.DataFrame(
        [
            {'database': 'dev', 'name': 'cool', 'columns': '{"name":"foo", "type":"bar"}'},
            {'database': 'dev', 'name': 'cool', 'columns': '{"name":"roo", "type":"far"}'},
        ]
    )
    mock_tables_response = pd.DataFrame(
        [
            {
                'database': 'dev',
                'schema': 'public',
                'type': 'table',
                'name': 'cool',
            }
        ]
    )
    mock_get_connection().__enter__().cursor().__enter__().fetchall.return_value = [('dev',)]
    mock_get_connection().__enter__().cursor().__enter__().fetch_dataframe.side_effect = [
        mock_cols_response,
        mock_tables_response,
    ]
    assert redshift_connector.get_model(DATABASE_NAME) == [
        {
            'database': 'dev',
            'schema': 'public',
            'name': 'cool',
            'type': 'table',
            'columns': [{'name': 'foo', 'type': 'bar'}, {'name': 'roo', 'type': 'far'}],
        }
    ]
