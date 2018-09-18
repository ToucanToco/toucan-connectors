from toucan_connectors.http_api.http_api_connector import HttpAPIConnector, HttpAPIDataSource

def test_get_df():

    HC = HttpAPIConnector(name = "myHttpConnector", type ="HttpAPI", baseroute = "https://jsonplaceholder.typicode.com")
    HD = HttpAPIDataSource(name = "myHttpDataSource", domain = "my_domain", url= "/posts", )

    df = HC.get_df(HD)
    
    assert df.shape == (100,4)
