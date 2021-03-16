import pytest

from toucan_connectors.facebookads.facebookads_connector import (
    FacebookadsConnector,
    FacebookadsDataSource,
)
from toucan_connectors.facebookads.helpers import FacebookadsDataKind


@pytest.fixture
def connector():
    return FacebookadsConnector(
        name='Facebook Ads test connector',
        token='tkn',
        account_id='1092812098',
    )


@pytest.fixture
def data_source():
    return FacebookadsDataSource(
        name='Facebook Ads sample data source',
        domain='domain',
        data_kind=FacebookadsDataKind.campaigns,
        parameters={},
    )


@pytest.fixture
def http_get_mock(mocker):
    return mocker.patch('requests.get')


def test_facebookads(connector, data_source, http_get_mock):
    df = connector.get_df(data_source)

    given_url, given_kwargs = http_get_mock.call_args
    assert http_get_mock.called_once()
    assert given_url[0] == f'https://graph.facebook.com/v10.0/act_{connector.account_id}/campaigns'
    assert given_kwargs['params'] == {'access_token': 'tkn'}

    assert df.empty


def test_facebookads_apply_query_params(connector, data_source, http_get_mock):
    data_source.parameters = {
        'date_preset': 'today',
    }

    connector.get_df(data_source)

    _, kwargs = http_get_mock.call_args
    assert http_get_mock.called_once()
    assert 'date_preset' in kwargs['params']
    assert kwargs['params']['date_preset'] == 'today'


def test_facebookads_apply_unauthorized_query_params(connector, data_source, http_get_mock):
    data_source.parameters = {'pizza': 'pizza'}

    connector.get_df(data_source)

    url, kwargs = http_get_mock.call_args
    assert http_get_mock.called_once
    assert 'pizza' not in kwargs['params']


def test_facebookads_ads_under_campaign(connector, data_source, http_get_mock):
    data_source.data_kind = FacebookadsDataKind.ads_under_campaign

    connector.get_df(data_source)

    given_url, given_kwargs = http_get_mock.call_args
    assert http_get_mock.called_once()
    assert given_url[0] == f'https://graph.facebook.com/v10.0/{data_source.campaign_id}/ads'
    assert given_kwargs['params'] == {'access_token': 'tkn'}
