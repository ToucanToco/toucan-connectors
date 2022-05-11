from unittest.mock import patch

import pandas
import pytest
from google.cloud.bigquery import ArrayQueryParameter, Client, ScalarQueryParameter
from google.cloud.bigquery.job.query import QueryJob
from google.cloud.bigquery.table import RowIterator
from google.oauth2.service_account import Credentials
from pandas.util.testing import assert_frame_equal  # <-- for testing dataframes

from toucan_connectors.google_big_query.google_big_query_connector import (
    GoogleBigQueryConnector,
    GoogleBigQueryDataSource,
    _define_query_param,
)
from toucan_connectors.google_credentials import GoogleCredentials


@pytest.fixture
def _fixture_credentials():
    my_credentials = GoogleCredentials(
        type='my_type',
        project_id='my_project_id',
        private_key_id='my_private_key_id',
        private_key='my_private_key',
        client_email='my_client_email@email.com',
        client_id='my_client_id',
        auth_uri='https://accounts.google.com/o/oauth2/auth',
        token_uri='https://oauth2.googleapis.com/token',
        auth_provider_x509_cert_url='https://www.googleapis.com/oauth2/v1/certs',
        client_x509_cert_url='https://www.googleapis.com/robot/v1/metadata/x509/pika.com',
    )
    return my_credentials


@pytest.fixture
def _fixture_scope():
    scopes = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/drive',
    ]
    return scopes


@pytest.mark.parametrize(
    'input_value,expected_output',
    [
        ('test', ScalarQueryParameter('test_param', 'STRING', 'test')),
        (0, ScalarQueryParameter('test_param', 'NUMERIC', 0)),
        (0.0, ScalarQueryParameter('test_param', 'FLOAT64', 0.0)),
        (True, ScalarQueryParameter('test_param', 'BOOL', True)),
        ([], ArrayQueryParameter('test_param', 'STRING', [])),
        (['hi'], ArrayQueryParameter('test_param', 'STRING', ['hi'])),
        ([0], ArrayQueryParameter('test_param', 'NUMERIC', [0])),
        ([0.0, 2], ArrayQueryParameter('test_param', 'FLOAT64', [0.0, 2])),
        ([True, False], ArrayQueryParameter('test_param', 'BOOL', [True, False])),
    ],
)
def test__define_query_param(input_value, expected_output):
    assert _define_query_param('test_param', input_value) == expected_output


def test_prepare_query():
    query = 'SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = {{test_str}} AND test2 = {{test_float}} LIMIT 10'
    result = GoogleBigQueryConnector._prepare_query(query)
    assert (
        result
        == 'SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = @test_str AND test2 = @test_float LIMIT 10'
    )


def test_prepare_parameters():
    query = 'SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = {{test_str}} AND test2 = {{test_float}} LIMIT 10'
    new_query = GoogleBigQueryConnector._prepare_query(query)
    parameters = GoogleBigQueryConnector._prepare_parameters(
        new_query,
        {
            'test_str': str('tortank'),
            'test_int': int(1),
            'test_float': float(0.0),
            'test_bool': True,
        },
    )
    assert len(parameters) == 2
    assert parameters[0] == ScalarQueryParameter('test_str', 'STRING', 'tortank')
    assert parameters[1] == ScalarQueryParameter('test_float', 'FLOAT64', 0.0)


def test_prepare_parameters_empty():
    query = 'SELECT stuff FROM `useful-citizen-322414.test.test`'
    new_query = GoogleBigQueryConnector._prepare_query(query)
    parameters = GoogleBigQueryConnector._prepare_parameters(new_query, None)
    assert len(parameters) == 0


@patch('google.cloud.bigquery.Client', autospec=True)
@patch('cryptography.hazmat.primitives.serialization.load_pem_private_key')
def test_connect(load_pem_private_key, client, _fixture_credentials, _fixture_scope):
    credentials = GoogleBigQueryConnector._get_google_credentials(
        _fixture_credentials, _fixture_scope
    )
    assert isinstance(credentials, Credentials)
    connection = GoogleBigQueryConnector._connect(credentials)
    assert isinstance(connection, Client)


@patch(
    'google.cloud.bigquery.table.RowIterator.to_dataframe',
    return_value=pandas.DataFrame({'a': [1, 1], 'b': [2, 2]}),
)
@patch('google.cloud.bigquery.job.query.QueryJob.result', return_value=RowIterator)
@patch('google.cloud.bigquery.Client.query', return_value=QueryJob)
@patch('google.cloud.bigquery.Client', autospec=True)
def test_execute(client, execute, result, to_dataframe):
    result = GoogleBigQueryConnector._execute_query(client, 'SELECT 1 FROM my_table', [])
    assert_frame_equal(pandas.DataFrame({'a': [1, 1], 'b': [2, 2]}), result)


@patch(
    'google.cloud.bigquery.table.RowIterator.to_dataframe',
    return_value=pandas.DataFrame({'a': [1, 1], 'b': [2, 2]}),
)
@patch('google.cloud.bigquery.job.query.QueryJob.result', return_value=RowIterator)
@patch('google.cloud.bigquery.Client.query', side_effect=TypeError)
@patch('google.cloud.bigquery.Client', autospec=True)
def test_execute_error(client, execute, result, to_dataframe):
    with pytest.raises(TypeError):
        GoogleBigQueryConnector._execute_query(client, 'SELECT 1 FROM my_table', [])


@patch(
    'toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._get_google_credentials',
    return_value=Credentials,
)
@patch(
    'toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._connect',
    return_value=Client,
)
@patch(
    'toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._execute_query',
    return_value=pandas.DataFrame({'a': [1, 1], 'b': [2, 2]}),
)
def test_retrieve_data(execute, connect, credentials, _fixture_credentials):
    connector = GoogleBigQueryConnector(
        name='MyGBQ',
        credentials=_fixture_credentials,
        scopes=[
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/drive',
        ],
    )
    datasource = GoogleBigQueryDataSource(
        name='MyGBQ',
        domain='wiki',
        query="SELECT * FROM bigquery-public-data:samples.wikipedia WHERE test = '{{key}}' LIMIT 1000",
        parameters={'key': 'tortank'},
    )
    result = connector._retrieve_data(datasource)
    assert_frame_equal(pandas.DataFrame({'a': [1, 1], 'b': [2, 2]}), result)
