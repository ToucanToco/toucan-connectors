import os

import pandas as pd
import pytest
from pydantic import SecretStr

from toucan_connectors.awsathena.awsathena_connector import AwsathenaConnector, AwsathenaDataSource


@pytest.fixture
def athena_connector():
    return AwsathenaConnector(
        name='test',
        s3_output_bucket='s3://test/results/',
        aws_access_key_id='test_access_key_id',
        aws_secret_access_key=SecretStr('test_secret_access_key'),
        region_name='test-region',
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
    read_sql_query_mocked = mocker.patch(
        'awswrangler.athena.read_sql_query', return_value=fixture_csv
    )

    # Mocking because the comparison of two boto3.Session objects is always false
    # We cannot mock get_session on the athena_connector instance directly, because
    # pydantic models alter getattr behaviour
    boto_session_mocked = mocker.patch.object(
        AwsathenaConnector, 'get_session', return_value={'a': 'b'}
    )

    # The actual data request
    df = athena_connector.get_df(data_source=sample_data_source)

    assert df.equals(fixture_csv)

    read_sql_query_mocked.assert_called_once_with(
        'SELECT * FROM beers', database='mydatabase', boto3_session={'a': 'b'}, params={}
    )

    boto_session_mocked.assert_called_once()


def test_get_session(athena_connector):
    sess = athena_connector.get_session()
    assert sess.region_name == 'test-region'
    creds = sess.get_credentials()
    assert creds.access_key == 'test_access_key_id'
    assert creds.secret_key == SecretStr('test_secret_access_key')
