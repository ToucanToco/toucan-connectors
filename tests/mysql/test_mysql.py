import collections

import numpy as np
import pandas as pd
import pymysql
import pytest
from pydantic import ValidationError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.mysql.mysql_connector import MySQLConnector, MySQLDataSource


@pytest.fixture(scope='module')
def mysql_server(service_container):
    def check(host_port):
        conn = pymysql.connect(
            host='127.0.0.1', port=host_port, user='ubuntu', password='ilovetoucan'
        )
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        cur.close()
        conn.close()

    return service_container('mysql', check, pymysql.Error)


@pytest.fixture
def mysql_connector(mysql_server):
    return MySQLConnector(
        name='mycon',
        host='localhost',
        port=mysql_server['port'],
        user='ubuntu',
        password='ilovetoucan',
    )


def test_datasource():
    with pytest.raises(ValidationError):
        MySQLDataSource(name='mycon', domain='mydomain', database='mysql_db', query='')

    with pytest.raises(ValueError) as exc_info:
        MySQLDataSource(name='mycon', domain='mydomain', database='mysql_db')
    assert "'query' or 'table' must be set" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        MySQLDataSource(
            name='mycon', domain='mydomain', database='mysql_db', query='myquery', table='mytable'
        )
    assert "Only one of 'query' or 'table' must be set" in str(exc_info.value)

    MySQLDataSource(name='mycon', domain='mydomain', database='mysql_db', table='mytable')
    MySQLDataSource(name='mycon', domain='mydomain', database='mysql_db', query='myquery')


def test_get_connection_params():
    connector = MySQLConnector(name='my_mysql_con', host='myhost', user='myuser')
    params = connector.get_connection_params()
    params.pop('conv')
    assert params == {
        'host': 'myhost',
        'user': 'myuser',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }

    connector = MySQLConnector(
        name='my_mssql_con',
        host='myhost',
        user='myuser',
        password='mypass',
        port=123,
        charset='utf8',
        connect_timeout=50,
    )
    params = connector.get_connection_params()
    params.pop('conv')
    assert params == {
        'host': 'myhost',
        'user': 'myuser',
        'charset': 'utf8',
        'cursorclass': pymysql.cursors.DictCursor,
        'password': 'mypass',
        'port': 123,
        'connect_timeout': 50,
    }


def test_get_status_all_good(mysql_connector):
    assert mysql_connector.get_status() == ConnectorStatus(
        status=True,
        details=[
            ('Hostname resolved', True),
            ('Port opened', True),
            ('Host connection', True),
            ('Authenticated', True),
        ],
    )


def test_get_status_bad_host(mysql_connector):
    mysql_connector.host = 'localhot'
    status = mysql_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Hostname resolved', False),
        ('Port opened', None),
        ('Host connection', None),
        ('Authenticated', None),
    ]
    assert status.error is not None


def test_get_status_bad_port(mysql_connector):
    mysql_connector.port = 123000
    status = mysql_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Hostname resolved', True),
        ('Port opened', False),
        ('Host connection', None),
        ('Authenticated', None),
    ]
    assert 'port must be 0-65535.' in status.error


def test_get_status_bad_connection(mysql_connector, unused_port, mocker):
    mysql_connector.port = unused_port()
    mocker.patch(
        'toucan_connectors.mysql.mysql_connector.MySQLConnector.check_port', return_value=True
    )
    status = mysql_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Hostname resolved', True),
        ('Port opened', True),
        ('Host connection', False),
        ('Authenticated', None),
    ]
    assert status.error.startswith("Can't connect to MySQL server on 'localhost'")


def test_get_status_bad_authentication(mysql_connector):
    mysql_connector.user = 'pika'
    assert mysql_connector.get_status() == ConnectorStatus(
        status=False,
        details=[
            ('Hostname resolved', True),
            ('Port opened', True),
            ('Host connection', True),
            ('Authenticated', False),
        ],
        error="Access denied for user 'pika'@'172.17.0.1' (using password: YES)",
    )


def test_get_df(mocker):
    """ It should call the sql extractor """
    snock = mocker.patch('pymysql.connect')
    reasq = mocker.patch('pandas.read_sql')
    mocker.patch(
        'toucan_connectors.mysql.mysql_connector.MySQLConnector.get_foreign_key_info'
    ).return_value = []

    data_sources_spec = [
        {
            'domain': 'MySQL test',
            'type': 'external_database',
            'name': 'Some MySQL provider',
            'database': 'mysql_db',
            'table': 'City',
        }
    ]
    mysql_connector = MySQLConnector(
        name='mycon', host='localhost', port=22, user='ubuntu', password='ilovetoucan'
    )

    data_source = MySQLDataSource(**data_sources_spec[0])
    mysql_connector.get_df(data_source)

    conv = pymysql.converters.conversions.copy()
    conv[246] = float
    snock.assert_called_once_with(
        host='localhost',
        user='ubuntu',
        database='mysql_db',
        password='ilovetoucan',
        port=22,
        charset='utf8mb4',
        conv=conv,
        cursorclass=pymysql.cursors.DictCursor,
    )

    reasq.assert_called_once_with('select * from City', con=snock(), params={})


def test_get_df_db_follow(mysql_connector):
    """" It should extract the table City and make some merge with some foreign key """
    data_sources_spec = [
        {
            'domain': 'MySQL test',
            'type': 'external_database',
            'name': 'Some MySQL provider',
            'database': 'mysql_db',
            'table': 'City',
            'follow_relations': True,
        }
    ]

    expected_columns = [
        'ID',
        'Name_City',
        'CountryCode',
        'District',
        'Population_City',
        'Name_Country',
        'Continent',
        'Region',
        'SurfaceArea',
        'IndepYear',
        'Population_Country',
        'LifeExpectancy',
        'GNP',
        'GNPOld',
        'LocalName',
        'GovernmentForm',
        'HeadOfState',
        'Capital',
        'Code2',
    ]

    data_source = MySQLDataSource(**data_sources_spec[0])
    df = mysql_connector.get_df(data_source)

    assert not df.empty
    assert len(df.columns) == 19

    assert collections.Counter(df.columns) == collections.Counter(expected_columns)
    assert len(df.columns) == len(expected_columns)

    assert len(df[df['Population_City'] > 5000000]) == 24


def test_get_df_db(mysql_connector):
    """" It should extract the table City without merges """
    data_source_spec = {
        'domain': 'MySQL test',
        'type': 'external_database',
        'name': 'Some MySQL provider',
        'database': 'mysql_db',
        'query': 'SELECT * FROM City WHERE Population > %(max_pop)s',
        'parameters': {'max_pop': 5000000},
    }

    expected_columns = {'ID', 'Name', 'CountryCode', 'District', 'Population'}
    data_source = MySQLDataSource(**data_source_spec)
    df = mysql_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_forbidden_table_interpolation(mysql_connector):
    data_source_spec = {
        'domain': 'MySQL test',
        'type': 'external_database',
        'name': 'Some MySQL provider',
        'database': 'mysql_db',
        'query': 'SELECT * FROM %(tablename)s WHERE Population > 5000000',
        'parameters': {'tablename': 'City'},
    }

    data_source = MySQLDataSource(**data_source_spec)
    with pytest.raises(pd.io.sql.DatabaseError) as e:
        mysql_connector.get_df(data_source)
    assert 'interpolating table name is forbidden' in str(e.value)


def test_clean_response():
    """ It should replace None by np.nan and decode bytes data """
    response = [{'name': 'fway', 'age': 13}, {'name': b'zbruh', 'age': None}]
    res = MySQLConnector.clean_response(response)

    assert len(res) == 2
    assert res[1]['name'] == 'zbruh'
    assert np.isnan(res[1]['age'])


def test_decode_df():
    """It should decode the bytes columns"""
    df = pd.DataFrame(
        {
            'date': [b'2013-08-01', b'2013-08-02'],
            'country': ['France', 'Germany'],
            'number': [1, 2],
            'other': [b'pikka', b'chuuu'],
            'random': [3, 4],
        }
    )
    res = MySQLConnector.decode_df(df)
    assert res['date'].tolist() == ['2013-08-01', '2013-08-02']
    assert res['other'].tolist() == ['pikka', 'chuuu']
    assert res[['country', 'number', 'random']].equals(df[['country', 'number', 'random']])

    df2 = df[['number', 'random']]
    res = MySQLConnector.decode_df(df2)
    assert res.equals(df2)


def test_get_form_empty_query(mysql_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = MySQLDataSource.get_form(mysql_connector, current_config)
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['information_schema', 'mysql_db'],
    }


def test_get_form_query_with_good_database(mysql_connector):
    """It should give suggestions of the collections"""
    current_config = {'database': 'mysql_db'}
    form = MySQLDataSource.get_form(mysql_connector, current_config)
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['information_schema', 'mysql_db'],
    }
    assert form['properties']['table'] == {'$ref': '#/definitions/table'}
    assert form['definitions']['table'] == {
        'title': 'table',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['City', 'Country', 'CountryLanguage'],
    }
