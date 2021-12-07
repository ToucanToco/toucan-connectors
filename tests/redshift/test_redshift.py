from unittest.mock import Mock, patch

from redshift_connector.error import InterfaceError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.redshift import redshift_database_connector
from toucan_connectors.redshift.redshift_database_connector import RedshiftConnector


def test_redshiftdatasource_get_form():
    instance = redshift_database_connector.RedshiftDataSource(
        database='test', table='test', domain='test', name='test'
    )
    current_config = {'database': 'redshift'}
    connector = Mock()
    result = instance.get_form(connector, current_config)
    assert result['properties']['database']['title'] == 'Database'
    assert result['properties']['table']['title'] == 'Table'
    assert result['properties']['validation']['title'] == 'Validation'
    assert result['required'] == ['domain', 'name', 'database']


def test_redshiftconnector_get_connection_params():
    instance = redshift_database_connector.RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    result = instance.get_connection_params()
    assert result == dict(
        database='test', host='localhost', user='user', cluster_identifier='test', port=0
    )


@patch('toucan_connectors.redshift.redshift_database_connector.redshift_connector')
def test_redshiftconnector_get_connection(mock_redshift_connector):
    instance = redshift_database_connector.RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    redshift_mock = Mock()
    mock_redshift_connector.connect.return_value = redshift_mock
    result = instance._get_connection()
    assert result == redshift_mock


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_cursor(mock_get_connection):
    instance = redshift_database_connector.RedshiftConnector(
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
    query = """Select * from database"""
    instance = redshift_database_connector.RedshiftConnector(
        name='test',
        database='test',
        host='localhost',
        user='user',
        cluster_identifier='test',
        port=0,
    )
    dataframe_mock = Mock()
    mock_redshift_connector.connect().__enter__().cursor().__enter__().fetch_dataframe.return_value = (
        dataframe_mock
    )
    result = instance._retrieve_data(query)
    assert result == dataframe_mock


@patch.object(RedshiftConnector, '_get_connection')
def test_redshiftconnector_get_status(mock_get_connection):
    instance = redshift_database_connector.RedshiftConnector(
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
    instance = redshift_database_connector.RedshiftConnector(
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
