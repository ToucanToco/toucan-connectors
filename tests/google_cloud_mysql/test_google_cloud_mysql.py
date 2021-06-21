import pymysql

from toucan_connectors.google_cloud_mysql.google_cloud_mysql_connector import (
    GoogleCloudMySQLConnector,
    GoogleCloudMySQLDataSource,
)


def test_connection_params():
    connector = GoogleCloudMySQLConnector(
        name='gcloud_sql_con', host='my_host', user='my_user', password='my_pass'
    )
    params = connector.get_connection_params()
    assert set(params) == {'host', 'password', 'charset', 'user', 'conv', 'cursorclass'}

    assert params['host'] == 'my_host'
    assert params['user'] == 'my_user'
    assert params['password'] == 'my_pass'
    assert params['charset'] == 'utf8mb4'
    assert params['cursorclass'] == pymysql.cursors.DictCursor


def test_connection_params_default_pw():
    connector = GoogleCloudMySQLConnector(name='gcloud_sql_con', host='my_host', user='my_user')
    params = connector.get_connection_params()
    assert set(params) == {'host', 'password', 'charset', 'user', 'conv', 'cursorclass'}

    assert params['host'] == 'my_host'
    assert params['user'] == 'my_user'
    assert params['password'] == ''
    assert params['charset'] == 'utf8mb4'
    assert params['cursorclass'] == pymysql.cursors.DictCursor


def test_gcmysql_get_df(mocker):
    snock = mocker.patch('pymysql.connect')
    reasq = mocker.patch('pandas.read_sql')

    mysql_connector = GoogleCloudMySQLConnector(
        name='test', host='localhost', port=22, user='ubuntu', password='ilovetoucan'
    )
    ds = GoogleCloudMySQLDataSource(
        domain='test', name='test', database='mysql_db', query='my_query'
    )
    mysql_connector.get_df(ds)

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
    reasq.assert_called_once_with('my_query', con=snock(), params=None)
