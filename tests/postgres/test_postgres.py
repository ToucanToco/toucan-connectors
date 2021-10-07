import pandas as pd
import psycopg2
import pytest
from pydantic import ValidationError

from toucan_connectors.postgres.postgresql_connector import (
    PostgresConnector,
    PostgresDataSource,
    pgsql,
)


@pytest.fixture(scope='module')
def postgres_server(service_container):
    def check(host_port):
        conn = psycopg2.connect(
            host='127.0.0.1',
            port=host_port,
            database='postgres_db',
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
    """It should raise an error as no user is given"""
    with pytest.raises(ValidationError):
        PostgresConnector(host='some_host', name='test')


def test_open_connection():
    """It should not open a connection"""
    with pytest.raises(psycopg2.OperationalError):
        ds = PostgresDataSource(domain='pika', name='pika', database='circle_test', query='q')
        PostgresConnector(name='test', host='lolcathost', user='ubuntu', connect_timeout=1).get_df(
            ds
        )


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        PostgresDataSource(domaine='test', name='test', database='ubuntu', query='')


def test_datasource():
    with pytest.raises(ValidationError):
        PostgresDataSource(name='mycon', domain='mydomain', database='ubuntu', query='')

    with pytest.raises(ValueError) as exc_info:
        PostgresDataSource(name='mycon', domain='mydomain', database='ubuntu')
    assert "'query' or 'table' must be set" in str(exc_info.value)

    ds = PostgresDataSource(name='mycon', domain='mydomain', database='ubuntu', table='test')
    assert ds.query == 'select * from test;'


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
    """It should connect to the database and retrieve the response to the query"""
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
    """It should extract the table City and make some merge with some foreign key."""
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


def test_get_df_db_jinja_syntax(postgres_connector):
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM City WHERE Population > {{ max_pop }}',
        'parameters': {'max_pop': 5000000},
    }
    expected_columns = {'id', 'name', 'countrycode', 'district', 'population'}
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_forbidden_table_interpolation(postgres_connector):
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM %(tablename)s WHERE Population > 5000000',
        'parameters': {'tablename': 'City'},
    }
    data_source = PostgresDataSource(**data_source_spec)
    with pytest.raises(pd.io.sql.DatabaseError) as e:
        postgres_connector.get_df(data_source)
    assert 'interpolating table name is forbidden' in str(e.value)


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


def test_get_form_empty_query(postgres_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = PostgresDataSource.get_form(postgres_connector, current_config)
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['postgres', 'postgres_db'],
    }


def test_get_form_query_with_good_database(postgres_connector, mocker):
    """It should give suggestions of the collections"""
    current_config = {'database': 'postgres_db'}
    form = PostgresDataSource.get_form(postgres_connector, current_config)
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['postgres', 'postgres_db'],
    }
    assert form['properties']['table'] == {'$ref': '#/definitions/table'}
    assert form['definitions']['table'] == {
        'title': 'table',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['city', 'country', 'countrylanguage'],
    }
    assert form['required'] == ['domain', 'name', 'database']


def test_get_form_connection_fails(mocker, postgres_connector):
    """It should return a form even if connect fails"""
    mocker.patch.object(pgsql, 'connect').side_effect = IOError
    form = PostgresDataSource.get_form(postgres_connector, current_config={})
    assert 'table' in form['properties']
