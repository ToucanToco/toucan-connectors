import os
from typing import Optional

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


@pytest.fixture
def data_source():
    return AwsathenaDataSource(
        name='test',
        domain='toto',
        database='mydatabase',
        table='beers',
    )


@pytest.fixture
def sample_data() -> pd.DataFrame:
    fixture_path = f'{os.path.dirname(__file__)}/fixtures/beers.csv'
    return pd.read_csv(fixture_path)


@pytest.fixture
def mocked_read_sql_query(mocker, sample_data):
    return mocker.patch('awswrangler.athena.read_sql_query', return_value=sample_data)


@pytest.fixture
def mocked_boto_session(mocker):
    # Mocking because the comparison of two boto3.Session objects is always false
    # We cannot mock get_session on the athena_connector instance directly, because
    # pydantic models alter getattr behaviour
    return mocker.patch.object(AwsathenaConnector, 'get_session', return_value={'a': 'b'})


def test_AwsathenaDataSource():
    s1 = AwsathenaDataSource(domain='d', name='source_one', database='db', table='coucou')
    assert s1.query == 'SELECT * FROM coucou;'

    s2 = AwsathenaDataSource(
        domain='d', name='source_two', database='db', query='SELECT * FROM coucou;'
    )
    assert s2.query == 'SELECT * FROM coucou;'
    assert s2.table is None

    with pytest.raises(ValueError, match="'table' or 'query' must be specified"):
        AwsathenaDataSource(domain='d', name='source_three', database='db')


def test_get_df(
    mocker, athena_connector, data_source, sample_data, mocked_read_sql_query, mocked_boto_session
):
    # The actual data request
    df = athena_connector.get_df(data_source=data_source)

    assert df.equals(sample_data)

    mocked_read_sql_query.assert_called_once_with(
        'SELECT * FROM beers;',
        database='mydatabase',
        boto3_session={'a': 'b'},
        params={},
        s3_output='s3://test/results/',
    )

    mocked_boto_session.assert_called_once()


def test_get_session(athena_connector):
    sess = athena_connector.get_session()
    assert sess.region_name == 'test-region'
    creds = sess.get_credentials()
    assert creds.access_key == 'test_access_key_id'
    assert creds.secret_key == 'test_secret_access_key'


def test_get_slice(
    mocker, athena_connector, data_source, mocked_read_sql_query, mocked_boto_session
):
    permissions = {'column': 'style', 'operator': 'in', 'value': ['Blonde', 'Triple']}
    result = athena_connector.get_slice(data_source, permissions=permissions, offset=10, limit=110)
    assert len(result.df) == result.stats.total_returned_rows == result.stats.total_rows == 4
    assert sorted(result.df['style'].unique().tolist()) == ['Blonde', 'Triple']
    mocked_read_sql_query.assert_called_once_with(
        'SELECT * FROM (SELECT * FROM beers) LIMIT 110 OFFSET 10;',
        database='mydatabase',
        boto3_session={'a': 'b'},
        params={},
        s3_output='s3://test/results/',
    )


@pytest.mark.parametrize(
    'query,offset,limit,expected',
    [
        # no pagination
        ('SELECT * FROM toto;', 0, None, 'SELECT * FROM toto;'),
        # limit only, whitespace, no trailing ;
        ('   SELECT * FROM toto ', 0, 100, 'SELECT * FROM (SELECT * FROM toto) LIMIT 100;'),
        # limit only, whitespace, with trailing ;
        ('  SELECT * FROM toto; ', 0, 100, 'SELECT * FROM (SELECT * FROM toto) LIMIT 100;'),
        # offset + limit, whitespace, no trailing ;
        ('  SELECT * FROM toto ', 5, 100, 'SELECT * FROM (SELECT * FROM toto) LIMIT 100 OFFSET 5;'),
        # offset + limit, whitespace, trailing ;
        (' SELECT * FROM toto; ', 5, 100, 'SELECT * FROM (SELECT * FROM toto) LIMIT 100 OFFSET 5;'),
    ],
)
def test_add_pagination_to_query(
    athena_connector, query: str, offset: int, limit: Optional[int], expected: str
):
    assert athena_connector._add_pagination_to_query(query, offset=offset, limit=limit) == expected
