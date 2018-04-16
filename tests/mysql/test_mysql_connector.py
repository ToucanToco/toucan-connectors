import collections

import numpy as np
import pandas as pd
import pymysql
import pytest

from toucan_connectors.mysql.mysql_connector import MySQLConnector, MySQLDataSource


@pytest.fixture(scope='module')
def mysql_server(service_container):
    def check(host_port):
        conn = pymysql.connect(host='127.0.0.1', port=host_port, database='mysql_db',
                               user='ubuntu', password='ilovetoucan')
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        cur.close()
        conn.close()

    return service_container('mysql', check, pymysql.Error)


@pytest.fixture
def mysql_connector(mysql_server):
    return MySQLConnector(name='mycon', host='localhost', db='mysql_db', port=mysql_server['port'],
                          user='ubuntu', password='ilovetoucan')


def test_datasource():
    with pytest.raises(ValueError) as exc_info:
        MySQLDataSource(name='mycon', domain='mydomain', query='')
    assert 'query:\n  length less than minimum allowed: 1' in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        MySQLDataSource(name='mycon', domain='mydomain')
    assert "'query' or 'table' must be set" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        MySQLDataSource(name='mycon', domain='mydomain', query='myquery', table='mytable')
    assert "Only one of 'query' or 'table' must be set" in str(exc_info.value)

    MySQLDataSource(name='mycon', domain='mydomain', table='mytable')
    MySQLDataSource(name='mycon', domain='mydomain', query='myquery')


def test_connection_params():
    connector = MySQLConnector(name='my_mysql_con', host='myhost', user='myuser', db='mydb')
    params = connector.connection_params
    params.pop('conv')
    assert params == {'host': 'myhost', 'user': 'myuser', 'database': 'mydb', 'charset': 'utf8mb4',
                      'cursorclass': pymysql.cursors.DictCursor}

    connector = MySQLConnector(name='my_mssql_con', host='myhost', user='myuser', db='mydb',
                               password='mypass', port=123, charset='utf8', connect_timeout=50)
    params = connector.connection_params
    params.pop('conv')
    assert params == {'host': 'myhost', 'user': 'myuser', 'database': 'mydb', 'charset': 'utf8',
                      'cursorclass': pymysql.cursors.DictCursor, 'password': 'mypass',
                      'port': 123, 'connect_timeout': 50}


def test_get_df(mysql_connector, mocker):
    """ It should call the sql extractor """
    mocker.patch('pandas.read_sql').return_value = pd.DataFrame({'a1': ['a', 'b'], 'b1': [1, 2]},
                                                                index=['ai', 'bi'])
    mocker.patch('pandas.DataFrame.merge').return_value = pd.DataFrame()
    mocker.patch('pandas.DataFrame.drop').return_value = pd.DataFrame()

    data_sources_spec = [
        {
            'domain': 'MySQL test',
            'type': 'external_database',
            'name': 'Some MySQL provider',
            'table': 'City'
        }
    ]

    data_source = MySQLDataSource(**data_sources_spec[0])
    df = mysql_connector.get_df(data_source)
    assert df.empty


def test_get_df_db(mysql_connector):
    """" It should extract the table City and make some merge with some foreign key """
    data_sources_spec = [
        {
            'domain': 'MySQL test',
            'type': 'external_database',
            'name': 'Some MySQL provider',
            'table': 'City'
        }
    ]

    expected_columns = ['ID', 'Name_City', 'CountryCode', 'District',
                        'Population_City', 'Name_Country', 'Continent',
                        'Region', 'SurfaceArea', 'IndepYear',
                        'Population_Country', 'LifeExpectancy', 'GNP',
                        'GNPOld', 'LocalName', 'GovernmentForm', 'HeadOfState',
                        'Capital', 'Code2']

    data_source = MySQLDataSource(**data_sources_spec[0])
    df = mysql_connector.get_df(data_source)

    assert not df.empty
    assert len(df.columns) == 19

    assert collections.Counter(df.columns) == collections.Counter(expected_columns)
    assert len(df.columns) == len(expected_columns)

    assert len(df[df['Population_City'] > 5000000]) == 24


def test_clean_response():
    """ It should replace None by np.nan and decode bytes data """
    response = [{'name': 'fway', 'age': 13}, {'name': b'zbruh', 'age': None}]
    res = MySQLConnector.clean_response(response)

    assert len(res) == 2
    assert res[1]['name'] == 'zbruh'
    assert np.isnan(res[1]['age'])
