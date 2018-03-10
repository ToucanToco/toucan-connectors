import collections

import numpy as np
import pandas as pd
import pymysql
import pytest

from toucan_connectors.abstract_connector import (
    BadParameters,
    UnableToConnectToDatabaseException,
    InvalidQuery
)
from toucan_connectors.mysql.mysql_connector import MySQLConnector


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
    return MySQLConnector(host='localhost', db='mysql_db', user='ubuntu', password='ilovetoucan',
                          port=mysql_server['port'])


def test_missing_params():
    """ It should throw a BadParameters error """
    with pytest.raises(BadParameters):
        MySQLConnector(host='localhost')


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        MySQLConnector(host='lolcathost', db='mysql_db',
                       user='ubuntu', connect_timeout=1).__enter__()


def test_retrieve_response(connector):
    """ It should connect to the database and retrieve the response to the query """
    with connector as mysql_connector:
        with pytest.raises(InvalidQuery):
            mysql_connector.query('')
        res = mysql_connector.query('SELECT Name, CountryCode, Population FROM City LIMIT 2;')
        assert isinstance(res, list)
        assert isinstance(res[0], dict)
        assert len(res[0]) == 3


def test_get_df(connector, mocker):
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

    with connector as mysql_connector:
        df = mysql_connector.get_df(data_sources_spec[0])
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

    with connector as mysql_connector:
        df = mysql_connector.get_df(data_sources_spec[0])

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
