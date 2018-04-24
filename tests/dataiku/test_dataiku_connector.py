import pytest
import responses

from toucan_connectors.dataiku.dataiku_connector import DataikuConnector, DataikuDataSource

dc = DataikuConnector(
    name='test',
    host='http://domain.dataiku.com:9876/',
    apiKey='',
    project='TOUCANTOCO'
)

ds = DataikuDataSource(
    name='test',
    domain='my_domain',
    dataset='my_dataset'
)


@responses.activate
def test_microstrategy():
    base_url = 'http://domain.dataiku.com:9876//dip/publicapi/'
    fmt = '?format=tsv-excel-header'
    responses.add(responses.GET,
                  f'{base_url}projects/{dc.project}/datasets/{ds.dataset}/data/{fmt}',
                  body='a\tb\n1\t2', status=200)

    df = dc.get_df(ds)
    assert df.shape == (1, 2)


@pytest.mark.skip(reason="It's alive")
def test_on_live_instance():
    df = dc.get_df(ds)
    assert df.shape == (153, 9)
