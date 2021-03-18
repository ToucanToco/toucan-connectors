from unittest.mock import call

import pytest

from toucan_connectors.facebook_ads.facebook_ads_connector import (
    FacebookAdsConnector,
    FacebookadsDataSource,
)
from toucan_connectors.facebook_ads.helpers import FacebookadsDataKind
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector


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


def test_facebook_ads(connector, data_source, http_get_mock):
    data_source.parameters['account_id'] = 'test_me'
    df = connector.get_df(data_source)

    given_url, given_kwargs = http_get_mock.call_args
    assert http_get_mock.called_once()
    assert given_url[0] == 'https://graph.facebook.com/v10.0/act_test_me/campaigns'
    assert given_kwargs['params']['access_token'] == 'tkn'

    assert df.empty


def test_facebook_ads_apply_query_params(connector, data_source, http_get_mock):
    data_source.parameters = {
        'date_preset': 'today',
    }

    connector.get_df(data_source)

    _, kwargs = http_get_mock.call_args
    assert http_get_mock.called_once()
    assert 'date_preset' in kwargs['params']
    assert kwargs['params']['date_preset'] == 'today'


def test_facebook_ads_ads_under_campaign(connector, data_source, http_get_mock):
    data_source.data_kind = FacebookadsDataKind.ads_under_campaign
    data_source.data_fields = 'name'

    connector.get_df(data_source)

    given_url, given_kwargs = http_get_mock.call_args
    assert http_get_mock.called_once()
    assert (
        given_url[0]
        == f'https://graph.facebook.com/v10.0/{data_source.parameters.get("campaign_id")}/ads'
    )
    assert given_kwargs['params']['access_token'] == 'tkn'
    assert given_kwargs['params']['fields'] == 'name'


def test_facebook_ads_handle_pagination(connector, data_source, http_get_mock, mocker):
    requests_json_mock = mocker.Mock()
    requests_json_mock.side_effect = [
        {'data': [{'foo': 'bar'}], 'paging': {'next': 'http://example.com/foo'}},
        {'data': [], 'paging': {}},
    ]
    http_get_mock.return_value.json = requests_json_mock

    df = connector.get_df(data_source)

    expected_calls = [
        call(
            (
                f'https://graph.facebook.com/v10.0//act_{data_source.parameters.get("account_id")}/campaigns',
            ),
            {'access_token': 'tkn'},
        ),
        call(('http://example.com/foo',), {'access_token': 'tkn'}),
    ]

    assert http_get_mock.call_count == 2
    assert http_get_mock.has_calls(expected_calls)
    assert df['foo'][0] == 'bar'


def test_facebook_ads_build_authorization_uri(connector, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    connector.build_authorization_url()

    mock_oauth2_connector.build_authorization_url.assert_called()


def test_facebook_ads_retrieve_tokens(connector, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    connector.retrieve_tokens('foo')

    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_facebook_ads_get_connector_secrets_form(connector, mocker):
    doc = connector.get_connector_secrets_form()
    assert doc is not None
