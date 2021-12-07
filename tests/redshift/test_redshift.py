import pandas as pd
import pytest

from botocore import exceptions
from unittest.mock import Mock
from toucan_connectors.common import ConnectorStatus
from toucan_connectors.redshift import redshift_database_connector
from toucan_connectors.redshift.redshift_database_connector import (
    RedshiftDataSource,
)


def test_redshiftdatasource_get_form():
    current_config = {'database': 'redshift'}
    form = RedshiftDataSource.get_form(redshift_database_connector, current_config)
    assert form['properties']['database']['title'] == 'Database'
    assert form['properties']['table']['title'] == 'Table'
    assert form['properties']['validation']['title'] == 'Validation'
    assert form['required'] == ['domain', 'name', 'database']


def test_redshiftconnector_get_connection_params():
    instance = redshift_database_connector.RedshiftConnector(
        name='test', dbname='test', host='localhost', user='user', cluster_identifier='test', port=0
    )
    result = instance.get_connection_params()
    assert result == dict(dbname='test', host='localhost', user='user', port=0)


def test_redshiftconnector_get_connection():
    ds = RedshiftDataSource(
        domain='test',
        name='test',
        database='redshift_db',
        host='localhost',
    )
    result = redshift_database_connector.RedshiftConnector(ds)._get_connection()
    assert isinstance(result, pd.DataFrame)
    assert result == '?'


def test_redshiftconnector_get_cursor():
    ds = RedshiftDataSource(
        domain='test',
        name='test',
        database='redshift_db',
    )
    instance = redshift_database_connector.RedshiftConnector(ds)
    mock_connection = Mock()
    instance._get_connection = mock_connection
    result = instance._get_cursor

    assert result == mock_connection


def test_redshiftconnector_retrieve_data():
    ds = RedshiftDataSource(name='test', database='redshift_db', domain='test')
    result = redshift_database_connector.RedshiftConnector(ds)._retrieve_data()
    assert isinstance(result, pd.DataFrame)
    assert result == '?'


def test_redshiftconnector_get_status():
    ds = RedshiftDataSource(name='test', database='redshift_db', domain='test')
    instance = redshift_database_connector.RedshiftConnector(ds)
    mock_connection = Mock()
    instance._get_connection = mock_connection
    result = instance.get_status
    assert result == ConnectorStatus(status=True, details='?', error=None)
    with pytest.raises(exceptions.ClusterNotFoundException):
        result = instance.get_status
    assert result == ConnectorStatus(status=False, details='?', error=None)
