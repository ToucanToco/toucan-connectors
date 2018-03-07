import pandas as pd
import psycopg2
import pytest

from connectors.abstract_connector import MissingConnectorOption
from connectors.postgres import PostgresConnector
from connectors.sql_connector import UnableToConnectToDatabaseException, InvalidSQLQuery


@pytest.fixture(scope='module', autouse=True)
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
def connector(postgres_server):
    return PostgresConnector(name='postgres', host='localhost', db='postgres_db',
                             user='ubuntu', password='ilovetoucan', port=postgres_server['port'])


def test_required_args():
    with pytest.raises(MissingConnectorOption):
        PostgresConnector(name='a_connector_has_no_name',
                          host='some_host',
                          bla='missing_something')

    connector = PostgresConnector(name='a_connector_has_no_name',
                                  host='localhost',
                                  db='circle_test',
                                  user='ubuntu',
                                  bla='missing_something')
    assert all(arg in connector.connection_params for arg in connector._get_required_args())
    assert 'bla' not in connector.connection_params


def test_normalized_args():
    connector = PostgresConnector(name='a_connnector_does_have_a_name',
                                  host='some_host',
                                  user='DennisRitchie',
                                  bla='missing_something',
                                  db='some_db')

    chargs = connector._changes_normalize_args()
    assert all(change in connector.connection_params for change in list(chargs.values()))
    assert 'db' not in connector.connection_params


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        PostgresConnector(name='pgsql',
                          host='lolcathost',
                          db='circle_test',
                          user='ubuntu',
                          connect_timeout=1).open_connection()


def test_retrieve_response(connector):
    """ It should connect to the database and retrieve the response to the query """
    with pytest.raises(InvalidSQLQuery):
        connector.query('')
    res = connector.query('SELECT Name, CountryCode, Population FROM City LIMIT 2;')
    assert isinstance(res, list)
    assert isinstance(res[0], tuple)
    assert len(res[0]) == 3


def test_get_df(connector, mocker):
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

    df = connector.get_df(data_sources_spec[0])
    assert df.shape == (2, 2)


def test_get_df_db(connector):
    """ It should extract the table City and make some merge with some foreign key. """
    data_sources_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'query': 'SELECT * FROM city;'
    }

    expected_columns = ['id', 'name', 'countrycode', 'district', 'population']

    df = connector.get_df(data_sources_spec)

    assert not df.empty
    assert len(df.columns) == len(expected_columns)
    assert len(df[df['population'] > 5000000]) == 24
