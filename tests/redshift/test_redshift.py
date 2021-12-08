from unittest.mock import Mock, patch

import pytest
from redshift_connector.error import InterfaceError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.redshift.redshift_database_connector import (
    RedshiftConnector,
    RedshiftDataSource,
)


@pytest.fixture
def redshift_connector():
    return RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftdatasource_get_form(mock_redshift_connector, redshift_connector):
    instance = RedshiftDataSource(database='test', table='test', domain='test', name='test')
    current_config = {'database': 'redshift'}
    mock_redshift_connector.connect().return_value = Mock()
    result = instance.get_form(redshift_connector, current_config)
    assert result['properties']['parameters']['title'] == 'Parameters'
    assert result['properties']['table']['title'] == 'Table'
    assert result['properties']['validation']['title'] == 'Validation'
    assert result['required'] == ['domain', 'name', 'database']


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftdatasource_get_form_with_error(mock_redshift_connector):
    instance = RedshiftDataSource(database='test', table='test', domain='test', name='test')
    mock_redshift_connector.connect.side_effect = InterfaceError
    connector = RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    result = instance.get_form(connector, {})
    assert result == {'title': 'FormSchema', 'type': 'object', 'properties': {}}


def test_redshiftconnector_get_connection_params():
    instance = RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    result = instance.get_connection_params(database='redshift_database')
    assert result == dict(
        database='redshift_database',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_get_connection(mock_redshift_connector):
    instance = RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    datasource = RedshiftDataSource(database='test', table='test', domain='test', name='test')
    redshift_mock = Mock()
    mock_redshift_connector.connect.return_value = redshift_mock
    result = instance._get_connection(datasource)
    assert result == redshift_mock


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_cursor(mock_get_connection):
    instance = RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    connection_mock = Mock()
    mock_get_connection().cursor.return_value = connection_mock
    result = instance._get_cursor()
    assert result == connection_mock


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_retrieve_data(mock_redshift_connector):
    instance = RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    datasource = RedshiftDataSource(database='test', table='test', domain='test', name='test')
    dataframe_mock = Mock()
    mock_redshift_connector.connect().__enter__().cursor().__enter__().fetch_dataframe.return_value = (
        dataframe_mock
    )
    result = instance._retrieve_data(datasource)
    assert result == dataframe_mock


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_status(mock_get_connection):
    instance = RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    response = {
        'Marker': 'string',
        'Clusters': [
            {
                'ClusterIdentifier': 'test_id1',
                'NodeType': 'test_type1',
                'ClusterStatus': 'status1',
            },
            {
                'ClusterIdentifier': 'test_id2',
                'NodeType': 'test_type2',
                'ClusterStatus': 'status2',
            },
            {
                'ClusterIdentifier': 'test_id3',
                'NodeType': 'test_type3',
                'ClusterStatus': 'status3',
            },
        ],
    }
    mock_get_connection().describe_clusters.return_value = response
    result = instance.get_status()
    assert result == ConnectorStatus(
        status=True, details=[('status1',), ('status2',), ('status3',)], error=None
    )


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_status_with_error(mock_get_connection):
    instance = RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    mock_get_connection().describe_clusters.side_effect = InterfaceError
    result = instance.get_status()
    assert result == ConnectorStatus(status=False, error=InterfaceError)
