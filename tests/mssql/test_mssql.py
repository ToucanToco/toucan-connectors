import os

import pandas as pd
import pydantic
import pymssql
import pytest

from toucan_connectors.mssql.mssql_connector import MSSQLConnector, MSSQLDataSource


@pytest.fixture(scope='module')
def mssql_server(service_container):
    def check_and_feed(host_port):
        """
        This method does not only check that the server is on
        but also feeds the database once it's up !
        """
        conn = pymssql.connect(host='127.0.0.1', port=host_port, user='SA', password='Il0veT0uc@n!')
        cur = conn.cursor()
        cur.execute('SELECT 1;')

        # Feed the database
        sql_query_path = f'{os.path.dirname(__file__)}/fixtures/world.sql'
        with open(sql_query_path) as f:
            sql_query = f.read()
        cur.execute(sql_query)
        conn.commit()

        cur.close()
        conn.close()

    return service_container('mssql', check_and_feed, pymssql.Error)


@pytest.fixture
def mssql_connector(mssql_server):
    return MSSQLConnector(
        name='mycon',
        host='localhost',
        user='SA',
        password='Il0veT0uc@n!',
        port=mssql_server['port'],
    )


@pytest.fixture
def mssql_datasource():
    def f(query):
        return MSSQLDataSource(name='mycon', domain='mydomain', query=query)

    return f


def test_datasource(mssql_datasource):
    with pytest.raises(pydantic.ValidationError):
        mssql_datasource(query='')
    mssql_datasource(query='ok')


def test_connection_params():
    connector = MSSQLConnector(name='my_mssql_con', host='myhost', user='myuser')
    assert connector.get_connection_params(None) == {
        'server': 'myhost',
        'user': 'myuser',
        'as_dict': True,
    }
    connector = MSSQLConnector(
        name='my_mssql_con',
        host='myhost',
        user='myuser',
        password='mypass',
        port=123,
        connect_timeout=60,
    )
    assert connector.get_connection_params('mydb') == {
        'server': 'myhost',
        'user': 'myuser',
        'as_dict': True,
        'password': 'mypass',
        'port': 123,
        'login_timeout': 60,
        'database': 'mydb',
    }


def test_mssql_get_df(mocker):
    snock = mocker.patch('pymssql.connect')
    reasq = mocker.patch('pandas.read_sql')

    mssql_connector = MSSQLConnector(
        name='mycon', host='localhost', user='SA', password='Il0veT0uc@n!', port=22
    )
    datasource = MSSQLDataSource(
        name='mycon',
        domain='mydomain',
        database='mydb',
        query='SELECT Name, CountryCode, Population ' 'FROM City WHERE ID BETWEEN 1 AND 3',
    )
    mssql_connector.get_df(datasource)

    snock.assert_called_once_with(
        as_dict=True,
        server='localhost',
        user='SA',
        password='Il0veT0uc@n!',
        port=22,
        database='mydb',
    )

    reasq.assert_called_once_with(
        'SELECT Name, CountryCode, Population FROM City WHERE ID BETWEEN 1 AND 3', con=snock()
    )


def test_get_df(mssql_connector, mssql_datasource):
    """ It should connect to the default database and retrieve the response to the query """
    datasource = mssql_datasource(
        query='SELECT Name, CountryCode, Population ' 'FROM City WHERE ID BETWEEN 1 AND 3'
    )
    expected = pd.DataFrame(
        {'Name': ['Kabul', 'Qandahar', 'Herat'], 'Population': [1780000, 237500, 186800]}
    )
    expected['CountryCode'] = 'AFG'
    expected = expected[['Name', 'CountryCode', 'Population']]

    # LIMIT 2 is not possible for MSSQL
    res = mssql_connector.get_df(datasource)
    res['Name'] = res['Name'].str.rstrip()
    assert res.equals(expected)
