import requests
import pytest

from toucan_connectors.elasticsearch.elasticsearch_connector import (
    ElasticsearchConnector, ElasticsearchDataSource
)


@pytest.fixture(scope='module')
def elasticsearch(service_container):
    def check_and_feed(host_port):
        """
        This method check that the server is on
        and feeds the database once it's up
        """
        url = f"http://localhost:{host_port}"
        requests.get(url)
        # Feed the database
        requests.put(url + "/company")
        requests.post(url + "/company/employees/1",
                      json={
                          "name": "Toto",
                          "best_song": "Africa"
                      })
        requests.post(url + "/company/employees/2",
                      json={
                          "name": "BRMC",
                          "best_song": "Beat The Devil\'s Tattoo",
                          "adress": {
                              "street": "laaa",
                              "cedex": 15,
                              "city": "looo"
                          }
                      })
        requests.post(url + "/company/_refresh")

    return service_container('elasticsearch', check_and_feed)


def test_connector(mocker):
    class ElasticsearchMock:
        def search(self, index, body):
            return {'hits': {'hits': [{'_source': {'yo': 'la'}}]}}

    module = 'toucan_connectors.elasticsearch.elasticsearch_connector'
    mock_es = mocker.patch(f'{module}.Elasticsearch')
    mock_es.return_value = ElasticsearchMock()

    con = ElasticsearchConnector(
        name='test',
        hosts=[
            {
                'url': 'https://toto.com/lu',
                'username': 'test',
                'password': 'test',
                'headers': {'truc': ''}
            }
        ]
    )
    ds = ElasticsearchDataSource(
        domain='test',
        name='test',
        index='_all',
        search_method='search',
        body={
            "_source": True
        }
    )
    con.get_df(ds)
    mock_es.assert_called_once_with(
        [{'host': 'toto.com', 'url_prefix': '/lu', 'port': 443, 'use_ssl': True,
          'http_auth': 'test:test', 'headers': {'truc': ''}}],
        send_get_body_as=None
    )


def test_get_df(elasticsearch):
    con = ElasticsearchConnector(
        name='test',
        hosts=[
            {
                'url': 'http://localhost',
                'port': elasticsearch['port']
            }
        ]
    )
    ds_search = ElasticsearchDataSource(
        domain='test',
        name='test',
        index='_all',
        search_method='search',
        body={
            "_source": True
        }
    )

    ds_msearch = ElasticsearchDataSource(
        domain='test',
        name='test',
        search_method='msearch',
        body=[
            {},
            {"_source": ['adress.city', 'best_song']}
        ]
    )
    data = con.get_df(ds_search)
    assert 'name' in data.columns
    assert 'adress.city' in data.columns
    assert all(data.loc[data['name'] == 'BRMC', 'best_song'] == 'Beat The Devil\'s Tattoo')

    data = con.get_df(ds_msearch)
    assert list(data.columns) == ['adress.city', 'best_song']
    assert all(data.loc[data['adress.city'].isnull(), 'best_song'] == 'Africa')
