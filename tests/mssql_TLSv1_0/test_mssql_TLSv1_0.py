import os

import pandas as pd
import pydantic
import pyodbc
import pytest

from toucan_connectors.mssql_TLSv1_0.mssql_connector import MSSQLConnector, MSSQLDataSource


@pytest.fixture(scope='module')
def mssql_server(service_container):
    def check_and_feed(host_port):
        """
        This method does not only check that the server is on
        but also feeds the database once it's up !
        """
        conn = pyodbc.connect(
            driver='{ODBC Driver 18 for SQL Server}',
            server=f'127.0.0.1,{host_port}',
            user='SA',
            password='Il0veT0uc@n!',
        )
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

    return service_container('mssql', check_and_feed, pyodbc.Error)


@pytest.fixture
def mssql_connector(mssql_server):
    return MSSQLConnector(
        name='mycon',
        host='localhost',
        user='SA',
        password='Il0veT0uc@n!',
        port=mssql_server['port'],
    )


def test_datasource():
    with pytest.raises(pydantic.ValidationError):
        MSSQLDataSource(name='mycon', domain='mydomain', database='ubuntu', query='')

    with pytest.raises(ValueError) as exc_info:
        MSSQLDataSource(name='mycon', domain='mydomain', database='ubuntu')
    assert "'query' or 'table' must be set" in str(exc_info.value)

    ds = MSSQLDataSource(name='mycon', domain='mydomain', database='ubuntu', table='test')
    assert ds.query == 'select * from test;'


@pytest.mark.skip(reason='TLS install script fails')
def test_connection_params():
    connector = MSSQLConnector(name='my_mssql_con', host='myhost', user='myuser')
    assert connector.get_connection_params(None) == {
        'driver': '{ODBC Driver 18 for SQL Server}',
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
        'driver': '{ODBC Driver 18 for SQL Server}',
        'server': 'myhost,123',
        'user': 'myuser',
        'as_dict': True,
        'password': 'mypass',
        'timeout': 60,
        'database': 'mydb',
    }


@pytest.mark.skip(reason='TLS install script fails')
def test_mssql_get_df(mocker):
    snock = mocker.patch('pyodbc.connect')
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
        driver='{ODBC Driver 18 for SQL Server}',
        as_dict=True,
        server='127.0.0.1,22',
        user='SA',
        password='Il0veT0uc@n!',
        database='mydb',
    )

    reasq.assert_called_once_with(
        'SELECT Name, CountryCode, Population FROM City WHERE ID BETWEEN 1 AND 3',
        con=snock(),
        params=[],
    )


@pytest.mark.skip(reason='TLS install script fails')
def test_get_df(mssql_connector):
    """It should connect to the default database and retrieve the response to the query"""
    datasource = MSSQLDataSource(
        name='mycon',
        domain='mydomain',
        query='SELECT Name, CountryCode, Population ' 'FROM City WHERE ID BETWEEN 1 AND 3',
        database='master',
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


@pytest.mark.skip(reason='TLS install script fails')
def test_query_variability(mocker):
    """It should connect to the database and retrieve the response to the query"""
    mock_pyodbc_connect = mocker.patch('pyodbc.connect')
    mock_pandas_read_sql = mocker.patch('pandas.read_sql')
    con = MSSQLConnector(
        name='mycon', host='localhost', user='SA', password='Il0veT0uc@n!', port=22
    )

    # Test with integer values
    ds = MSSQLDataSource(
        query='select * from test where id_nb > %(id_nb)s and price > %(price)s;',
        domain='test',
        name='test',
        parameters={'price': 10, 'id_nb': 1},
        database='db',
    )
    con.get_df(ds)
    mock_pandas_read_sql.assert_called_once_with(
        'select * from test where id_nb > ? and price > ?;',
        con=mock_pyodbc_connect(),
        params=[1, 10],
    )

    # Test when the value is an array
    mock_pandas_read_sql = mocker.patch('pandas.read_sql')
    ds = MSSQLDataSource(
        query='select * from test where id_nb in %(ids)s;',
        domain='test',
        name='test',
        database='db',
        parameters={'ids': [1, 2]},
    )
    con.get_df(ds)
    mock_pandas_read_sql.assert_called_once_with(
        'select * from test where id_nb in (?,?);',
        con=mock_pyodbc_connect(),
        params=[1, 2],
    )


@pytest.mark.skip(reason='TLS install script fails')
def test_query_variability_jinja(mocker):
    """It should interpolate safe (server side) parameters using jinja templating"""
    mock_pyodbc_connect = mocker.patch('pyodbc.connect')
    mock_pandas_read_sql = mocker.patch('pandas.read_sql')
    con = MSSQLConnector(
        name='mycon', host='localhost', user='SA', password='Il0veT0uc@n!', port=22
    )
    ds = MSSQLDataSource(
        query='select * from {{user.attributes.table_name}} where id_nb in %(ids)s;',
        domain='test',
        name='test',
        database='db',
        parameters={'ids': [1, 2], 'user': {'attributes': {'table_name': 'blah'}}},
    )
    con.get_df(ds)
    mock_pandas_read_sql.assert_called_once_with(
        'select * from blah where id_nb in (?,?);',
        con=mock_pyodbc_connect(),
        params=[1, 2],
    )


@pytest.mark.skip(reason='TLS install script fails')
def test_get_form_empty_query(mssql_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = MSSQLDataSource.get_form(mssql_connector, current_config)

    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['master', 'tempdb', 'model', 'msdb'],
    }


@pytest.mark.skip(reason='TLS install script fails')
def test_get_form_query_with_good_database(mssql_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {'database': 'master'}
    form = MSSQLDataSource.get_form(mssql_connector, current_config)

    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['master', 'tempdb', 'model', 'msdb'],
    }
    assert form['properties']['table'] == {'$ref': '#/definitions/table'}
    assert 'City' in form['definitions']['table']['enum']
    assert form['required'] == ['domain', 'name', 'database']
