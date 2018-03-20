import collections
import numpy as np
import pandas as pd
import cx_Oracle
import pytest
import os

from toucan_connectors.abstract_connector import (
    BadParameters,
    UnableToConnectToDatabaseException,
    InvalidQuery
)
from toucan_connectors.oracle import OracleConnector


@pytest.fixture(scope='module')
def oracle_server(service_container):
    def check(host_port):
        conn = cx_Oracle.connect(user='sys', password='ilovetoucan',
                                 dsn=f'127.0.0.1:{host_port}/oracle_db')
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")

        sql_query_path = f'{os.path.dirname(__file__)}/fixtures/world.sql'
        with open(sql_query_path) as f:
            sql_query = f.read()
        cursor.execute(sql_query)
        conn.commit()

        cursor.close()
        conn.close()

    return service_container('oracle', check, cx_Oracle.Error)


@pytest.fixture()
def oracle_connector(oracle_server):
    return OracleConnector(host='127.0.0.1', user='sys',
                           password='ilovetoucan', db='oracle_db')


def test_missing_params():
    """ It should throw a BadParameters error """
    with pytest.raises(BadParameters):
        OracleConnector(user='yo')


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        OracleConnector(
            host='127.0.0.1', user='sys', password='ilovetoucan', db='oracle_db'
        ).__enter__()


def test_retrieve_response(oracle_connector):
    """ It should connect to the database and retrieve the response to the query """
    with pytest.raises(InvalidQuery):
        oracle_connector.query('')
    res = oracle_connector.query('SELECT Name, CountryCode, Population FROM City LIMIT 2;')
    assert isinstance(res, list)
    assert isinstance(res[0], dict)
    assert len(res[0]) == 3


def test_get_df(oracle_connector, mocker):
    """ It should call the sql extractor """
    mocker.patch('pandas.read_sql').return_value = pd.DataFrame({'a1': ['a', 'b'], 'b1': [1, 2]},
                                                                index=['ai', 'bi'])
    mocker.patch('pandas.DataFrame.merge').return_value = pd.DataFrame()
    mocker.patch('pandas.DataFrame.drop').return_value = pd.DataFrame()

    data_sources_spec = [
        {
            'domain': 'Oracle test',
            'type': 'external_database',
            'name': 'Some Oracle provider',
            'table': 'City'
        }
    ]

    df = oracle_connector.get_df(data_sources_spec[0])
    assert df.empty


def test_get_df_db(oracle_connector):
    """" It should extract the table City and make some merge with some foreign key """
    data_sources_spec = [
        {
            'domain': 'Oracle test',
            'type': 'external_database',
            'name': 'Some Oracle provider',
            'table': 'City'
        }
    ]

    expected_columns = ['ID', 'Name_City', 'CountryCode', 'District',
                        'Population_City', 'Name_Country', 'Continent',
                        'Region', 'SurfaceArea', 'IndepYear',
                        'Population_Country', 'LifeExpectancy', 'GNP',
                        'GNPOld', 'LocalName', 'GovernmentForm', 'HeadOfState',
                        'Capital', 'Code2']

    df = oracle_connector.get_df(data_sources_spec[0])

    assert not df.empty
    assert len(df.columns) == 19

    assert collections.Counter(df.columns) == collections.Counter(expected_columns)
    assert len(df.columns) == len(expected_columns)

    assert len(df[df['Population_City'] > 5000000]) == 24


def test_clean_response():
    """ It should replace None by np.nan and decode bytes data """
    response = [{'name': 'fway', 'age': 13}, {'name': b'zbruh', 'age': None}]
    res = OracleConnector.clean_response(response)

    assert len(res) == 2
    assert res[1]['name'] == 'zbruh'
    assert np.isnan(res[1]['age'])
