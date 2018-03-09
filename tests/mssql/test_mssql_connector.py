import os
import pymssql

import pandas as pd
import pytest

from connectors.abstract_connector import (
    BadParameters,
    UnableToConnectToDatabaseException,
    InvalidQuery,
    MissingQueryParameter
)
from connectors.mssql import MSSQLConnector


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


@pytest.fixture()
def connector(mssql_server):
    return MSSQLConnector(host='localhost', user='SA', password='Il0veT0uc@n!',
                          port=mssql_server['port'])


def test_missing_params():
    """ It should throw a BadParameters error """
    with pytest.raises(BadParameters):
        MSSQLConnector(host='localhost')


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        MSSQLConnector(host='lolcathost', db='mssql_db', user='SA', connect_timeout=1).__enter__()


def test_retrieve_response(connector):
    """ It should connect to the database and retrieve the response to the query """
    query = 'SELECT Name, CountryCode, Population FROM City WHERE ID BETWEEN 1 AND 3'
    expected = pd.DataFrame({'Name': ['Kabul', 'Qandahar', 'Herat'],
                             'Population': [1780000, 237500, 186800]})
    expected['CountryCode'] = 'AFG'
    expected = expected[['Name', 'CountryCode', 'Population']]
    # test query method
    with connector as mssql_connector:
        with pytest.raises(InvalidQuery):
            mssql_connector.query('')
        # LIMIT 2 is not possible for MSSQL
        res = mssql_connector.query(query)
        res['Name'] = res['Name'].str.rstrip()
        assert res.equals(expected)

        with pytest.raises(MissingQueryParameter):
            mssql_connector.get_df(query)
        with pytest.raises(MissingQueryParameter):
            mssql_connector.get_df({'other': query})
        res = mssql_connector.get_df({'query': query})
        res['Name'] = res['Name'].str.rstrip()
        assert res.equals(expected)
