import pytest

from toucan_connectors.facebook_ads.facebook_ads_connector import (
    FacebookAdsConnector,
    FacebookadsDataSource,
)
from toucan_connectors.facebook_ads.helpers import FacebookadsDataKind


@pytest.fixture
def connector(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'tkn'})
    return FacebookAdsConnector(
        name='Facebook Ads test connector',
        secrets_keeper=secrets_keeper,
        redirect_uri='http://example.com/redirect',
        client_id='client_id',
        client_secret='s3cr3t',
        auth_flow_id='test',
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
    data_source.parameters['account_id'] = 'test_me'
    df = connector.get_df(data_source)

    given_url, given_kwargs = http_get_mock.call_args
    assert http_get_mock.called_once()
    assert given_url[0] == 'https://graph.facebook.com/v10.0/act_test_me/campaigns'
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


def test_facebookads_handle_pagination(connector, data_source, http_get_mock):
    assert False
