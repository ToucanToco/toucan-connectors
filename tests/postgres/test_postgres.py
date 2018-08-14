import pandas as pd
import psycopg2
from pydantic.exceptions import ValidationError
import pytest

from toucan_connectors.postgres.postgresql_connector import PostgresConnector, PostgresDataSource


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


@pytest.fixture
def postgres_connector(postgres_server):
    return PostgresConnector(name='test', host='localhost', db='postgres_db', user='ubuntu',
                             password='ilovetoucan', port=postgres_server['port'])


def test_no_user():
    """ It should raise an error as no user is given """
    with pytest.raises(ValidationError):
        PostgresConnector(host='some_host', name='test')


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(psycopg2.OperationalError):
        PostgresConnector(
                name='test',
                host='lolcathost',
                db='circle_test',
                user='ubuntu',
                connect_timeout=1).get_df({})


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        PostgresDataSource(domaine='test', name='test', query='')


@pytest.mark.skip(reason="This uses a live instance")
def test_retrieve_response(postgres_connector):
    """ It should connect to the database and retrieve the response to the query """
    ds = PostgresDataSource(
            domain='test',
            name='test',
            query='SELECT Name, CountryCode, Population FROM City LIMIT 2;')
    res = postgres_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert res.shape == (2, 3)


@pytest.mark.skip(reason="This uses a live instance")
def test_get_df_db(postgres_connector):
    """ It should extract the table City and make some merge with some foreign key. """
    data_sources_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'query': 'SELECT * FROM city;'
    }
    expected_columns = ['id', 'name', 'countrycode', 'district', 'population']

    df = postgres_connector.get_df(PostgresDataSource(**data_sources_spec))

    assert not df.empty
    assert len(df.columns) == len(expected_columns)
    assert len(df[df['population'] > 5000000]) == 24
