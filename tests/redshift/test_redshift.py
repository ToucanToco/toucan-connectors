from unittest.mock import Mock

import pytest
import pandas as pd

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.redshift import redshift_connector
from toucan_connectors.redshift.redshift_connector import RedshiftDataSource


def test_redshiftdatasource_get_form():
    current_config = {'database': 'redshift'}
    form = RedshiftDataSource.get_form(redshift_connector, current_config)
    assert form['properties']['database']['title'] == 'Database'
    assert form['properties']['table']['title'] == 'Table'
    assert form['properties']['validation']['title'] == 'Validation'
    assert form['required'] == ['domain', 'name', 'database']


def test_redshiftconnector_get_connection_params():
    instance = redshift_connector.RedshiftConnector()
    instance.dbname = 'dbname'
    instance.user = 'user'
    instance.password = 'password'
    instance.host = 'host'
    instance.port = 0
    instance.connect_timeout = 5000
    result = instance.get_connection_params
    assert result == {
        'dbname': 'dbname',
        'user': 'user',
        'password': 'password',
        'host': 'host',
        'port': 0,
        'connect_timeout': 5000,
    }


def test_redshiftconnector_get_connection():
    ds = RedshiftDataSource(
        domain='test', name='test', dbname='redshift_db', user='test', port=5000
    )
    result = redshift_connector.RedshiftConnector()._get_connection(ds)
    assert isinstance(result, pd.DataFrame)
    assert result == '?'


def test_redshiftconnector_get_cursor():
    instance = redshift_connector.RedshiftConnector()
    mock_connection = Mock()
    instance._get_connection = mock_connection
    result = instance._get_cursor
    assert result == mock_connection


def test_redshiftconnector_retrieve_data():
    ds = RedshiftDataSource(
        domain='test', name='test', dbname='redshift_db', user='test', port=5000
    )
    result = redshift_connector.RedshiftConnector()._retrieve_data(ds)
    assert isinstance(result, pd.DataFrame)
    assert result == '?'


def test_redshiftconnector_get_status():
    instance = redshift_connector.RedshiftConnector()
    mock_connection = Mock()
    instance._get_connection = mock_connection
    result = instance.get_status
    assert result == ConnectorStatus(status=True, details='?', error=None)
    with pytest.raises(exceptions.ClusterNotFoundException):
        result = instance.get_status
    assert result == ConnectorStatus(status=False, details='?', error=None)
