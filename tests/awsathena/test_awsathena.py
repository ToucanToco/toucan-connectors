import pytest
import pandas as pd
import os
import boto3

from toucan_connectors.awsathena.awsathena_connector import AwsathenaConnector, AwsathenaDataSource


@pytest.fixture
def athena_connector():
    return AwsathenaConnector(
        name='test',
        s3_output_bucket='s3://test/results/',
        aws_access_key_id='ascacsc',
        aws_secret_access_key='ascdscds09120983298sdcdsca',
        region_name='test-region'
    )


def test_get_df(mocker, athena_connector):
    sample_data_source = AwsathenaDataSource(
        name='test',
        domain='toto',
        database='mydatabase',
        query='SELECT * FROM beers',
    )

    fixture_path = f'{os.path.dirname(__file__)}/fixtures/beers.csv'
    fixture_csv = pd.read_csv(fixture_path)
    read_sql_query_mocked = mocker.patch('awswrangler.athena.read_sql_query', return_value=fixture_csv)

    boto_session_mocked = mocker.patch.object(athena_connector, 'get_session')
    boto_session_mocked.result({'titi': 'toto'})

    # The actual data request
    df = athena_connector.get_df(data_source=sample_data_source)
    assert df.equals(fixture_csv)

    read_sql_query_mocked.assert_called_once_with(
        'SELECT * FROM beers',
        database='mydatabase',
        boto3_session={'titi': 'toto'}
    )

    boto_session_mocked.assert_called_once()


def test_get_session():
    pass


def test_datasource():
    pass
