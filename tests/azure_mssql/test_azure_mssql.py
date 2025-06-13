from toucan_connectors.azure_mssql.azure_mssql_connector import (
    AzureMSSQLConnector,
    AzureMSSQLDataSource,
)


def test_connection_params():
    connector = AzureMSSQLConnector(host='my_host', user='my_user', name='')
    params = connector.get_connection_params()
    assert params['server'] == 'my_host.database.windows.net'
    assert params['user'] == 'my_user@my_host'

    connector = AzureMSSQLConnector(
        host='my_host.database.windows.net', user='my_user', password='', name=''
    )
    params = connector.get_connection_params()
    assert params['server'] == 'my_host.database.windows.net'
    assert params['user'] == 'my_user@my_host'

    connector = AzureMSSQLConnector(
        host='my_host.database.windows.net', user='my_user@my_host', password='', name=''
    )
    params = connector.get_connection_params()
    assert params['server'] == 'my_host.database.windows.net'
    assert params['user'] == 'my_user@my_host'


def test_gcmysql_get_df(mocker):
    snock = mocker.patch('pyodbc.connect')
    reasq = mocker.patch('pandas.read_sql')

    mssql_connector = AzureMSSQLConnector(
        name='test', host='localhost', user='ubuntu', password='ilovetoucan'
    )
    ds = AzureMSSQLDataSource(domain='test', name='test', database='mssql_db', query='my_query')
    mssql_connector.get_df(ds)

    snock.assert_called_once_with(
        server='localhost.database.windows.net',
        user='ubuntu@localhost',
        database='mssql_db',
        password='ilovetoucan',
        driver='{ODBC Driver 18 for SQL Server}',
    )
    reasq.assert_called_once_with('my_query', con=snock(), params=[])
