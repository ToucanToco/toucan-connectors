from toucan_connectors.hive.hive_connector import HiveConnector, HiveDataSource


def test_get_df():
    hc = HiveConnector(host='192.168.64.5', username='root', name='HiveTest')
    hds = HiveDataSource(domain='test', name='HiveTest', query='select * from test')
    df = hc.get_df(hds)
    assert df['test.a'].iloc[0] == 12
