import os
from typing import List, Optional

import pandas as pd
import pytest
from pydantic import SecretStr

from toucan_connectors.awsathena.awsathena_connector import AwsathenaConnector, AwsathenaDataSource
from toucan_connectors.common import ConnectorStatus


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


@pytest.fixture
def status_checks() -> List[str]:
    return [
        'Host resolved',
        'Port opened',
        'Connected',
        'Authenticated',
        'Can list databases',
    ]


def test_AwsathenaDataSource():
    s1 = AwsathenaDataSource(domain='d', name='source_one', database='db', table='coucou')
    assert s1.query == 'SELECT * FROM coucou;'

    s2 = AwsathenaDataSource(
        domain='d', name='source_two', database='db', query='SELECT * FROM coucou;'
    )
    assert s2.query == 'SELECT * FROM coucou;'
    assert s2.table is None

    # with parameters
    s3 = AwsathenaDataSource(
        domain='d',
        name='source_three',
        database='db',
        query='SELECT * FROM coucou WHERE toto > {{tata}}',
        parameters={'tata': 12345},
    )
    assert s3.query == 'SELECT * FROM coucou WHERE toto > 12345'

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
        s3_output='s3://test/results/',
    )


def test_get_status_list_dbs_ok(mocker, athena_connector, status_checks):
    mocker.patch('awswrangler.catalog.databases', return_value=pd.DataFrame())
    assert athena_connector.get_status() == ConnectorStatus(
        status=True, message=None, details=[(c, True) for c in status_checks], error=None
    )


def test_get_status_list_dbs_ko_sts_ok(mocker, athena_connector, status_checks):
    mocker.patch('awswrangler.catalog.databases', side_effect=Exception('coucou'))

    mocked_session = mocker.MagicMock()
    mocked_session.return_value.client.return_value.get_caller_identity.return_value = True
    mocker.patch.object(AwsathenaConnector, 'get_session', new=mocked_session)

    expected_details = [(c, True) for c in status_checks]
    expected_details[4] = ('Can list databases', False)

    assert athena_connector.get_status() == ConnectorStatus(
        status=True, message=None, details=expected_details, error='Cannot list databases: coucou'
    )


def test_get_status_ko(mocker, athena_connector, status_checks):
    mocker.patch('awswrangler.catalog.databases', side_effect=Exception('Insufficient permissions'))

    mocked_session = mocker.MagicMock()
    mocked_session.return_value.client.return_value.get_caller_identity.side_effect = Exception(
        'Authentication failed'
    )
    mocker.patch.object(AwsathenaConnector, 'get_session', new=mocked_session)

    # should failed
    assert athena_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error='Cannot verify connection to Athena: Insufficient permissions, Authentication failed',
        details=[(c, False) for c in status_checks],
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
