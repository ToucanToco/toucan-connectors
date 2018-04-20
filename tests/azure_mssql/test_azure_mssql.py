from toucan_connectors.azure_mssql.azure_mssql_connector import AzureMSSQLConnector


def test_connection_params():
    connector = AzureMSSQLConnector(host='my_host', user='my_user', password='', db='', name='')
    params = connector.connection_params
    assert params['server'] == 'my_host.database.windows.net'
    assert params['user'] == 'my_user@my_host'

    connector = AzureMSSQLConnector(host='my_host.database.windows.net', user='my_user',
                                    password='', db='', name='')
    params = connector.connection_params
    assert params['server'] == 'my_host.database.windows.net'
    assert params['user'] == 'my_user@my_host'

    connector = AzureMSSQLConnector(host='my_host.database.windows.net', user='my_user@my_host',
                                    password='', db='', name='')
    params = connector.connection_params
    assert params['server'] == 'my_host.database.windows.net'
    assert params['user'] == 'my_user@my_host'
