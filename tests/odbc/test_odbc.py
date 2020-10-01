import pandas as pd
import psycopg2
import pyodbc
import pytest
from pydantic import ValidationError

from toucan_connectors.odbc.odbc_connector import OdbcConnector, OdbcDataSource


def test_postgres_driver_installed():
    """
    This test aims to check that pgodbc is installed on the server
    """
    assert 'PostgreSQL Unicode' in pyodbc.drivers()



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
def odbc_connector(postgres_server):
    return OdbcConnector(
        name='test',
        connection_string=(
            'DRIVER={PostgreSQL Unicode};'
            'DATABASE=postgres_db;'
            'UID=ubuntu;'
            'PWD=ilovetoucan;'
            'SERVER=127.0.0.1;'
            'PORT=' + str(postgres_server['port']) + ';'
        ),
    )


def test_invalid_connection_string():
    """ It should raise an error as the connection string is invalid"""
    with pytest.raises(ValidationError):
        OdbcConnector(name='test')


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        OdbcDataSource(domain='test', name='test', database='postgres_db', query='')


def test_odbc_get_df(mocker):
    mockdbc = mocker.patch('pyodbc.connect')
    mockdas = mocker.patch('pandas.read_sql')

    odbc_connector = OdbcConnector(name='test', connection_string='blah')

    ds = OdbcDataSource(
        domain='test',
        name='test',
        query='SELECT Name, CountryCode, Population  from city LIMIT 2;',
    )
    odbc_connector.get_df(ds)

    mockdbc.assert_called_once_with('blah', autocommit=False, ansi=False)

    mockdas.assert_called_once_with(
        'SELECT Name, CountryCode, Population  from city LIMIT 2;', con=mockdbc(), params=[]
    )


def test_retrieve_response(odbc_connector):
    """ It should connect to the database and retrieve the response to the query """
    ds = OdbcDataSource(query='select * from City;', domain='test', name='test')

    res = odbc_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert res.shape[0] > 1


def test_query_variability(mocker):
    """ It should connect to the database and retrieve the response to the query """
    mockdbc = mocker.patch('pyodbc.connect')
    mockdas = mocker.patch('pandas.read_sql')
    odbc_connector = OdbcConnector(name='test', connection_string='blah')

    ds = OdbcDataSource(
        query='select * from test where id_nb > %(id_nb)s and price > %(price)s;',
        domain='test',
        name='test',
        parameters={'price': 10, 'id_nb': 1},
    )

    odbc_connector.get_df(ds)

    mockdas.assert_called_once_with(
        'select * from test where id_nb > ? and price > ?;', con=mockdbc(), params=[1, 10]
    )
