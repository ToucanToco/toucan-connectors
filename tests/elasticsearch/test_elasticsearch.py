import pytest
import requests
from pytest_mock import MockerFixture

from toucan_connectors.elasticsearch.elasticsearch_connector import (
    ElasticsearchConnector,
    ElasticsearchDataSource,
)


@pytest.fixture(scope="module")
def elasticsearch(service_container, request):
    def check_and_feed(host_port):
        """
        This method check that the server is on
        and feeds the database once it's up
        """
        url = f"http://localhost:{host_port}"
        requests.get(url)
        # Feed the database
        requests.post(url + "/employees/_create/1", json={"name": "Toto", "best_song": "Africa"})
        requests.post(
            url + "/employees/_create/2",
            json={
                "name": "BRMC",
                "best_song": "Beat The Devil's Tattoo",
                "adress": {"street": "laaa", "cedex": 15, "city": "looo"},
            },
        )
        requests.post(url + "/_refresh")

    return service_container(request.param, check_and_feed)


# parametrizing all tests depending on elasticsearch to use both
# elasticsearch 7, 8 and 9 containers
def pytest_generate_tests(metafunc):
    if "elasticsearch" in metafunc.fixturenames:
        metafunc.parametrize("elasticsearch", ["elasticsearch7", "elasticsearch8", "elasticsearch9"], indirect=True)


def test_connector(mocker: MockerFixture):
    module = "toucan_connectors.elasticsearch.elasticsearch_connector"
    mock_es = mocker.patch(f"{module}.Elasticsearch")
    mock_es.return_value.search.return_value = {"hits": {"hits": [{"_source": {"yo": "la"}}]}}

    con = ElasticsearchConnector(
        name="test",
        hosts=[
            {
                "url": "https://toto.com/lu",
                "username": "test",
                "scheme": "https",
                "password": "pikapika",
                "headers": {"truc": "bidule", "Accept": "override"},
            }
        ],
    )
    ds = ElasticsearchDataSource(
        domain="test", name="test", index="_all", search_method="search", body={"_source": True}
    )
    con.get_df(ds)
    mock_es.assert_called_once_with(
        [
            {
                "host": "toto.com",
                "url_prefix": "/lu",
                "port": 443,
                "scheme": "https",
            }
        ],
        basic_auth=("test", "pikapika"),
        headers={
            "truc": "bidule",
            # Accept header should have been converted to lowercase and overridden
            "accept": "override",
            # Content-type should have been added
            "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    )
    mock_es.return_value.perform_request.assert_called_once_with(
        "POST",
        "/_all/_search",
        body={"_source": True},
        endpoint_id="search",
        headers={
            "truc": "bidule",
            "accept": "override",
            "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    )


def test_get_df(elasticsearch):
    con = ElasticsearchConnector(name="test", hosts=[{"url": "http://localhost", "port": elasticsearch["port"]}])
    ds_search = ElasticsearchDataSource(
        domain="test", name="test", index="_all", search_method="search", body={"_source": True}
    )

    ds_msearch = ElasticsearchDataSource(
        domain="test",
        name="test",
        search_method="msearch",
        body=[{}, {"_source": ["adress.city", "best_song"]}],
    )
    data = con.get_df(ds_search)
    assert "name" in data.columns
    assert "adress.city" in data.columns
    assert all(data.loc[data["name"] == "BRMC", "best_song"] == "Beat The Devil's Tattoo")

    data = con.get_df(ds_msearch)
    assert set(data.columns) == {"adress.city", "best_song"}
    assert all(data.loc[data["adress.city"].isnull(), "best_song"] == "Africa")


def test_get_agg(elasticsearch):
    con = ElasticsearchConnector(name="test", hosts=[{"url": "http://localhost", "port": elasticsearch["port"]}])
    ds_search = ElasticsearchDataSource(
        domain="test",
        name="test",
        index="_all",
        search_method="search",
        body={
            "aggs": {
                "music": {"terms": {"field": "best_song.keyword"}},
                "sum_cedex": {"sum": {"field": "adress.cedex"}},
            }
        },
    )

    # Buckets + Metric
    expected = [
        {
            "music_buckets_doc_count": 1,
            "music_buckets_key": "Africa",
            "music_doc_count_error_upper_bound": 0,
            "music_sum_other_doc_count": 0,
            "sum_cedex_value": 15.0,
        },
        {
            "music_buckets_doc_count": 1,
            "music_buckets_key": "Beat The Devil's Tattoo",
            "music_doc_count_error_upper_bound": 0,
            "music_sum_other_doc_count": 0,
            "sum_cedex_value": 15.0,
        },
    ]
    data_search = con.get_df(ds_search)
    assert data_search.to_dict(orient="records") == expected

    # Multiple Buckets
    ds_msearch = ElasticsearchDataSource(
        domain="test",
        name="test",
        search_method="msearch",
        body=[
            {},
            {
                "aggs": {
                    "music": {
                        "terms": {"field": "best_song.keyword"},
                        "aggs": {"ville": {"terms": {"field": "adress.city.keyword"}}},
                    },
                    "ville": {"terms": {"field": "adress.city.keyword"}},
                }
            },
        ],
    )
    expected = [
        {
            "ville_buckets_doc_count": 1.0,
            "ville_buckets_key": "looo",
            "ville_doc_count_error_upper_bound": 0.0,
            "ville_sum_other_doc_count": 0.0,
        },
        {
            "music_buckets_doc_count": 1.0,
            "music_buckets_key": "Beat The Devil's Tattoo",
            "music_buckets_ville_buckets_doc_count": 1.0,
            "music_buckets_ville_buckets_key": "looo",
            "music_buckets_ville_doc_count_error_upper_bound": 0.0,
            "music_buckets_ville_sum_other_doc_count": 0.0,
            "music_doc_count_error_upper_bound": 0.0,
            "music_sum_other_doc_count": 0.0,
        },
    ]
    data_msearch = con.get_df(ds_msearch)
    results = [v.dropna().to_dict() for k, v in data_msearch.iterrows()]
    results.sort(key=lambda d: d.get("music_buckets_doc_count", -1))

    assert results == expected

    # Metric
    ds_search = ElasticsearchDataSource(
        domain="test",
        name="test",
        index="_all",
        search_method="search",
        body={"aggs": {"sum_cedex": {"sum": {"field": "adress.cedex"}}}},
    )

    expected = [{"sum_cedex_value": 15.0}]
    data_search = con.get_df(ds_search)
    assert data_search.to_dict(orient="records") == expected
