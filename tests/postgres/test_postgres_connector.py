import pandas as pd
import psycopg2
import pytest

from toucan_connectors.abstract_connector import (
    BadParameters,
    UnableToConnectToDatabaseException,
    InvalidQuery
)
from toucan_connectors.postgres.postgresql_connector import PostgresConnector, MissingHostParameter


@pytest.fixture(scope='module')
def postgres_server(service_container):
    def check(host_port):
        conn = psycopg2.connect(host='127.0.0.1', port=host_port, database='postgres_db',
                                user='ubuntu', password='ilovetoucan')
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        cur.close()
        conn.close()

    return service_container('postgres', check, psycopg2.Error)


@pytest.fixture()
def postgres_connector(postgres_server):
    return PostgresConnector(host='localhost', db='postgres_db', user='ubuntu',
                             password='ilovetoucan', port=postgres_server['port'])


def test_no_user():
    """ It should raise an error as no user is given """
    with pytest.raises(BadParameters):
        PostgresConnector(host='some_host')


def test_normalized_args():
    """ It should raise an error as neither host nor hostname is given """
    with pytest.raises(MissingHostParameter) as exc_info:
        PostgresConnector(user='DennisRitchie')
    assert str(exc_info.value) == 'You need to give a host or a hostname in order to connect'


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        PostgresConnector(host='lolcathost', db='circle_test', user='ubuntu',
                          connect_timeout=1).__enter__()


def test_retrieve_response(postgres_connector):
    """ It should connect to the database and retrieve the response to the query """
    with pytest.raises(InvalidQuery):
        postgres_connector.query('')
    res = postgres_connector.query('SELECT Name, CountryCode, Population FROM City LIMIT 2;')
    assert isinstance(res, list)
    assert isinstance(res[0], tuple)
    assert len(res[0]) == 3


def test_get_df(postgres_connector, mocker):
    """ It should call the sql extractor """
    mocker.patch('pandas.read_sql').return_value = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    data_sources_spec = [
        {
            'domain': 'Postgres test',
            'type': 'external_database',
            'name': 'Some MySQL provider',
            'query': 'SELECT * FROM city;'
        }
    ]

    df = postgres_connector.get_df(data_sources_spec[0])
    assert df.shape == (2, 2)


def test_get_df_db(postgres_connector):
    """ It should extract the table City and make some merge with some foreign key. """
    data_sources_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'query': 'SELECT * FROM city;'
    }
    expected_columns = ['id', 'name', 'countrycode', 'district', 'population']

    df = postgres_connector.get_df(data_sources_spec)

    assert not df.empty
    assert len(df.columns) == len(expected_columns)
    assert len(df[df['population'] > 5000000]) == 24
