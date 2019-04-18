from toucan_connectors.elasticsearch.elasticsearch_connector import (
    ElasticsearchConnector, ElasticsearchDataSource
)


def test_get_df():
    con = ElasticsearchConnector(
        name='test',
        hosts=[
            {
                'url': 'http://localhost',
                'port': 9200
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
