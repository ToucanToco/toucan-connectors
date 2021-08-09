import numpy
import pytest

from toucan_connectors.hubspot.hubspot_connector import (
    HubspotConnector,
    HubspotConnectorException,
    HubspotDataSource,
)
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector


@pytest.fixture
def connector(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'access_token'})
    return HubspotConnector(
        name='test',
        auth_flow_id='test',
        client_id='client_id',
        client_secret='s3cr3t',
        secrets_keeper=secrets_keeper,
        redirect_uri='http://example.com/redirect',
    )


@pytest.fixture
def datasource():
    return HubspotDataSource(dataset='contacts', domain='oui', name='name', parameters={})


def test_hubspot_build_authorization_uri(connector, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    connector.build_authorization_url()

    mock_oauth2_connector.build_authorization_url.assert_called()


def test_hubspot_retrieve_tokens(connector, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    connector.retrieve_tokens('foo')

    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_get_connector_secrets_form(connector, mocker):
    doc = connector.get_connector_secrets_form()
    assert doc is not None


def test_hubspot(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')

    req_mock.return_value.json = lambda: {'contacts': [{'properties': {'foo': 42}}]}

    df = connector.get_df(datasource)

    assert req_mock.called_once()
    assert df['properties.foo'][0] == 42


def test_hubspot_empty_return(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')

    req_mock.return_value.json = lambda: {'contacts': []}

    df = connector.get_df(datasource)

    assert df.empty


def test_hubspot_wrong_data_return(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')

    req_mock.return_value.json = lambda: {'results': 'oui'}

    with pytest.raises(HubspotConnectorException):
        connector.get_df(datasource)
        assert req_mock.called_once()


def test_hubspot_retrieve_data_pagination(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')
    requests_json_mock = mocker.Mock()
    requests_json_mock.side_effect = [
        {
            'contacts': [{'properties': {'foo': 42}, 'not_property': 'moo'}],
            'paging': {
                'next': {
                    'after': '42',
                    'link': 'https://api.hubapi.com/crm/v3/objects/contacts?after=42',
                }
            },
        },
        {
            'contacts': [{'properties': {'foo': 1337}}],
        },
    ]
    req_mock.return_value.json = requests_json_mock

    df = connector.get_df(datasource)

    assert req_mock.call_count == 2
    # `foo` is prefixed by properties here because the denesting done by pandas
    assert df['properties.foo'][0] == 42
    assert df['properties.foo'][1] == 1337
    assert df['not_property'][0] == 'moo'
    assert numpy.isnan(df['not_property'][1])
