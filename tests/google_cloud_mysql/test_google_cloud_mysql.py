import pymysql

from toucan_connectors.google_cloud_mysql.google_cloud_mysql_connector import (
    GoogleCloudMySQLConnector
)


def test_connection_params():
    connector = GoogleCloudMySQLConnector(name='gcloud_sql_con', host='my_host', user='my_user',
                                          db='my_db', password='my_pass')
    params = connector.connection_params
    assert set(params) == {'host', 'password', 'charset', 'database', 'user', 'conv', 'cursorclass'}

    assert params['host'] == 'my_host'
    assert params['user'] == 'my_user'
    assert params['database'] == 'my_db'
    assert params['password'] == 'my_pass'
    assert params['charset'] == 'utf8mb4'
    assert params['cursorclass'] == pymysql.cursors.DictCursor
