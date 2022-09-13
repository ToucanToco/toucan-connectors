from unittest.mock import patch

import pandas
import pandas as pd
import pytest
from google.cloud.bigquery import ArrayQueryParameter, Client, ScalarQueryParameter
from google.cloud.bigquery.job.query import QueryJob
from google.cloud.bigquery.table import RowIterator
from google.oauth2.service_account import Credentials
from pandas.util.testing import assert_frame_equal  # <-- for testing dataframes
from pytest_mock import MockFixture

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
        (0, ScalarQueryParameter('test_param', 'INT64', 0)),
        (0.0, ScalarQueryParameter('test_param', 'FLOAT64', 0.0)),
        (True, ScalarQueryParameter('test_param', 'BOOL', True)),
        ([], ArrayQueryParameter('test_param', 'STRING', [])),
        (['hi'], ArrayQueryParameter('test_param', 'STRING', ['hi'])),
        ([0], ArrayQueryParameter('test_param', 'INT64', [0])),
        ([0.0, 2], ArrayQueryParameter('test_param', 'FLOAT64', [0.0, 2])),
        ([True, False], ArrayQueryParameter('test_param', 'BOOL', [True, False])),
    ],
)
def test__define_query_param(input_value, expected_output):
    assert _define_query_param('test_param', input_value) == expected_output


def test_prepare_query_parameters():
    query = 'SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = {{test_str}} AND test2 = {{test_float}} LIMIT 10'
    new_query, parameters = GoogleBigQueryConnector._prepare_query_and_parameters(
        query,
        {
            'test_str': str('tortank'),
            'test_int': int(1),
            'test_float': float(0.0),
            'test_bool': True,
        },
    )
    assert len(parameters) == 2
    assert parameters[0] == ScalarQueryParameter('__QUERY_PARAM_0__', 'STRING', 'tortank')
    assert parameters[1] == ScalarQueryParameter('__QUERY_PARAM_1__', 'FLOAT64', 0.0)


def test_prepare_parameters_spaces():
    query = 'SELECT test, test2, test3 FROM `useful-citizen-322414.test.test` WHERE test = {{ test_str }} AND test2 = {{ test_float }} LIMIT 10'
    new_query, parameters = GoogleBigQueryConnector._prepare_query_and_parameters(
        query,
        {
            'test_str': str('tortank'),
            'test_int': int(1),
            'test_float': float(0.0),
            'test_bool': True,
        },
    )
    assert len(parameters) == 2
    assert parameters[0] == ScalarQueryParameter('__QUERY_PARAM_0__', 'STRING', 'tortank')
    assert parameters[1] == ScalarQueryParameter('__QUERY_PARAM_1__', 'FLOAT64', 0.0)


def test_prepare_parameters_empty():
    query = 'SELECT stuff FROM `useful-citizen-322414.test.test`'
    new_query, parameters = GoogleBigQueryConnector._prepare_query_and_parameters(query, None)
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


def test_get_model(mocker: MockFixture, _fixture_credentials) -> None:
    class FakeResponse:
        def __init__(self) -> None:
            ...

        def to_dataframe(self) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        'name': 'coucou',
                        'schema': 'foooo',
                        'database': 'myproject',
                        'type': 'BASE TABLE',
                        'column_name': 'pingu',
                        'data_type': 'STR',
                    },
                    {
                        'name': 'coucou',
                        'schema': 'foooo',
                        'database': 'myproject',
                        'type': 'BASE TABLE',
                        'column_name': 'toto',
                        'data_type': 'INT64',
                    },
                    {
                        'name': 'coucou',
                        'schema': 'foooo',
                        'database': 'myproject',
                        'type': 'BASE TABLE',
                        'column_name': 'tante',
                        'data_type': 'STR',
                    },
                    {
                        'name': 'blabla',
                        'schema': 'baarrrr',
                        'database': 'myproject',
                        'type': 'MATERIALIZED VIEW',
                        'column_name': 'gogo',
                        'data_type': 'STR',
                    },
                    {
                        'name': 'blabla',
                        'schema': 'baarrrr',
                        'database': 'myproject',
                        'type': 'MATERIALIZED VIEW',
                        'column_name': 'gaga',
                        'data_type': 'INT64',
                    },
                    {
                        'name': 'blabla',
                        'schema': 'baarrrr',
                        'database': 'myproject',
                        'type': 'MATERIALIZED VIEW',
                        'column_name': 'gg',
                        'data_type': 'STR',
                    },
                    {
                        'name': 'tortuga',
                        'schema': 'taar',
                        'database': 'myproject',
                        'type': 'VIEW',
                        'column_name': 'hammer',
                        'data_type': 'STR',
                    },
                    {
                        'name': 'tortuga',
                        'schema': 'taar',
                        'database': 'myproject',
                        'type': 'VIEW',
                        'column_name': 'to',
                        'data_type': 'INT64',
                    },
                    {
                        'name': 'tortuga',
                        'schema': 'taar',
                        'database': 'myproject',
                        'type': 'VIEW',
                        'column_name': 'fall',
                        'data_type': 'STR',
                    },
                ]
            )

    datasets = [
        mocker.MagicMock(dataset_id='foooo'),
        mocker.MagicMock(dataset_id='baarrrr'),
        mocker.MagicMock(dataset_id='taar'),
    ]

    mocker.patch.object(Client, 'list_datasets', return_value=datasets)
    mocked_query = mocker.patch.object(Client, 'query', return_value=FakeResponse())
    mocker.patch(
        'toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._connect',
        return_value=Client,
    )

    mocker.patch(
        'toucan_connectors.google_big_query.google_big_query_connector.GoogleBigQueryConnector._get_google_credentials',
        return_value=Credentials,
    )
    connector = GoogleBigQueryConnector(
        name='MyGBQ',
        credentials=_fixture_credentials,
        scopes=[
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/drive',
        ],
    )
    assert connector.get_model() == [
        {
            'name': 'blabla',
            'schema': 'baarrrr',
            'database': 'myproject',
            'type': 'view',
            'columns': [
                {'name': 'gogo', 'type': 'str'},
                {'name': 'gaga', 'type': 'int64'},
                {'name': 'gg', 'type': 'str'},
            ],
        },
        {
            'name': 'coucou',
            'schema': 'foooo',
            'database': 'myproject',
            'type': 'table',
            'columns': [
                {'name': 'pingu', 'type': 'str'},
                {'name': 'toto', 'type': 'int64'},
                {'name': 'tante', 'type': 'str'},
            ],
        },
        {
            'name': 'tortuga',
            'schema': 'taar',
            'database': 'myproject',
            'type': 'view',
            'columns': [
                {'name': 'hammer', 'type': 'str'},
                {'name': 'to', 'type': 'int64'},
                {'name': 'fall', 'type': 'str'},
            ],
        },
    ]
    assert (
        mocked_query.call_args_list[0][0][0]
        == "select C.table_name as name, C.table_schema as schema, T.table_catalog as database,\n                T.table_type as type,  C.column_name, C.data_type from foooo.INFORMATION_SCHEMA.COLUMNS C\n                JOIN foooo.INFORMATION_SCHEMA.TABLES T on C.table_name = T.table_name\n                where IS_SYSTEM_DEFINED='NO' AND IS_PARTITIONING_COLUMN='NO' AND IS_HIDDEN='NO'\nUNION ALL\nselect C.table_name as name, C.table_schema as schema, T.table_catalog as database,\n                T.table_type as type,  C.column_name, C.data_type from baarrrr.INFORMATION_SCHEMA.COLUMNS C\n                JOIN baarrrr.INFORMATION_SCHEMA.TABLES T on C.table_name = T.table_name\n                where IS_SYSTEM_DEFINED='NO' AND IS_PARTITIONING_COLUMN='NO' AND IS_HIDDEN='NO'\nUNION ALL\nselect C.table_name as name, C.table_schema as schema, T.table_catalog as database,\n                T.table_type as type,  C.column_name, C.data_type from taar.INFORMATION_SCHEMA.COLUMNS C\n                JOIN taar.INFORMATION_SCHEMA.TABLES T on C.table_name = T.table_name\n                where IS_SYSTEM_DEFINED='NO' AND IS_PARTITIONING_COLUMN='NO' AND IS_HIDDEN='NO'"
    )


def test_get_form(_fixture_credentials: MockFixture) -> None:
    assert (
        GoogleBigQueryDataSource(query=',', name='MyGBQ', domain='foo').get_form(
            GoogleBigQueryConnector(
                name='MyGBQ',
                credentials=_fixture_credentials,
                scopes=[
                    'https://www.googleapis.com/auth/bigquery',
                    'https://www.googleapis.com/auth/drive',
                ],
            ),
            {},
        )['properties']['database']['default']
        == 'my_project_id'
    )
