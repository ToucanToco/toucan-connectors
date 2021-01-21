import pandas as pd
import psycopg2
import pytest
from pydantic import ValidationError

from toucan_connectors.postgres.postgresql_connector import PostgresConnector, PostgresDataSource


@pytest.fixture(scope='module')
def postgres_server(service_container):
    def check(host_port):
        conn = psycopg2.connect(
            host='127.0.0.1',
            port=host_port,
            database='postgres',
            user='ubuntu',
            password='ilovetoucan',
        )
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        cur.close()
        conn.close()

    return service_container('postgres', check, psycopg2.Error)


@pytest.fixture
def postgres_connector(postgres_server):
    return PostgresConnector(
        name='test',
        host='localhost',
        user='ubuntu',
        password='ilovetoucan',
        port=postgres_server['port'],
    )


def test_no_user():
    """ It should raise an error as no user is given """
    with pytest.raises(ValidationError):
        PostgresConnector(host='some_host', name='test')


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(psycopg2.OperationalError):
        ds = PostgresDataSource(domain='pika', name='pika', database='circle_test', query='q')
        PostgresConnector(name='test', host='lolcathost', user='ubuntu', connect_timeout=1).get_df(
            ds
        )


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        PostgresDataSource(domaine='test', name='test', database='postgres_db', query='')


def test_postgress_get_df(mocker):
    snock = mocker.patch('psycopg2.connect')
    reasq = mocker.patch('pandas.read_sql')

    postgres_connector = PostgresConnector(
        name='test', host='localhost', user='ubuntu', password='ilovetoucan', port=22
    )

    ds = PostgresDataSource(
        domain='test',
        name='test',
        database='postgres_db',
        query='SELECT Name, CountryCode, Population FROM City LIMIT 2;',
    )
    postgres_connector.get_df(ds)

    snock.assert_called_once_with(
        host='localhost', dbname='postgres_db', user='ubuntu', password='ilovetoucan', port=22
    )

    reasq.assert_called_once_with(
        'SELECT Name, CountryCode, Population FROM City LIMIT 2;', con=snock(), params={}
    )


def test_retrieve_response(postgres_connector):
    """ It should connect to the database and retrieve the response to the query """
    ds = PostgresDataSource(
        domain='test',
        name='test',
        database='postgres_db',
        query='SELECT Name, CountryCode, Population FROM City LIMIT 2;',
    )
    res = postgres_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert res.shape == (2, 3)


def test_get_df_db(postgres_connector):
    """ It should extract the table City and make some merge with some foreign key. """
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM City WHERE Population > %(max_pop)s',
        'parameters': {'max_pop': 5000000},
    }
    expected_columns = {'id', 'name', 'countrycode', 'district', 'population'}
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_array_interpolation(postgres_connector):
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM City WHERE id in %(ids)s',
        'parameters': {'ids': [1, 2]},
    }
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)
    assert not df.empty
    assert df.shape == (2, 5)
