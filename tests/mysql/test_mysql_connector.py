import collections

import numpy as np
import pandas as pd
import pymysql
import pytest

from connectors.abstract_connector import MissingConnectorName, MissingConnectorOption
from connectors.mysql import MySQLConnector
from connectors.sql_connector import UnableToConnectToDatabaseException, InvalidSQLQuery


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


@pytest.fixture()
def connector(mysql_server):
    return MySQLConnector(name='mysql', host='localhost', db='mysql_db',
                          user='ubuntu', password='ilovetoucan', port=mysql_server['port'])


def test_missing_server_name():
    """ It should throw a missing connector name error """
    with pytest.raises(MissingConnectorName):
        MySQLConnector()


def test_missing_dict():
    """ It should throw a missing connector option error """
    with pytest.raises(MissingConnectorOption):
        MySQLConnector(name='mysql')


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        MySQLConnector(name='mysql', host='lolcathost', db='sql_db', user='ubuntu',
                       connect_timeout=1).open_connection()


def test_retrieve_response(connector):
    """ It should connect to the database and retrieve the response to the query """
    with pytest.raises(InvalidSQLQuery):
        connector.query('')
    res = connector.query('SELECT Name, CountryCode, Population FROM City LIMIT 2;')
    assert isinstance(res, list)
    assert isinstance(res[0], dict)
    assert len(res[0]) == 3


def test_get_df(connector, mocker):
    """ It should call the sql extractor """
    mock_read_sql = mocker.patch('pandas.read_sql')
    mock_read_sql.return_value = pd.DataFrame(list(dict(a=1, b=2).items()),
                                              columns=['a1', 'b1'], index=['ai', 'bi'])
    mock_merge = mocker.patch('pandas.DataFrame.merge')
    mock_merge.return_value = pd.DataFrame()
    mock_drop = mocker.patch('pandas.DataFrame.drop')
    mock_drop.return_value = pd.DataFrame()

    data_sources_spec = [
        {
            'domain': 'MySQL test',
            'type': 'external_database',
            'name': 'Some MySQL provider',
            'table': 'City'
        }
    ]

    df = connector.get_df(data_sources_spec[0])
    assert df.empty


def test_get_df_db(connector):
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

    df = connector.get_df(data_sources_spec[0])

    assert not df.empty
    assert len(df.columns) == 19

    assert collections.Counter(df.columns) == collections.Counter(expected_columns)
    assert len(df.columns) == len(expected_columns)

    assert len(df[df['Population_City'] > 5000000]) == 24


def test_clean_response(connector, mocker):
    """ It should replace None by np.nan and decode bytes data """
    connector._retrieve_response = mocker.MagicMock()
    connector._retrieve_response.return_value = [
        {'name': 'fway', 'age': 13},
        {'name': b'zbruh', 'age': None}
    ]
    res = connector.query('SELECT name, age from users')
    assert len(res) == 2
    assert res[1]['name'] == 'zbruh'
    assert np.isnan(res[1]['age'])
