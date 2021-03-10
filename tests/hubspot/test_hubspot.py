import numpy
import pytest

from toucan_connectors.hubspot.hubspot_connector import (
    HUBSPOT_ENDPOINTS,
    HubspotConnector,
    HubspotConnectorException,
    HubspotDataset,
    HubspotDataSource,
    HubspotObjectType,
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

    req_mock.return_value.json = lambda: {'results': [{'properties': {'foo': 42}}]}

    df = connector.get_df(datasource)

    assert req_mock.called_once()
    assert df['properties.foo'][0] == 42


def test_hubspot_empty_return(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')

    req_mock.return_value.json = lambda: {}

    df = connector.get_df(datasource)

    assert df.empty


def test_hubspot_empty_return_emails(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')

    req_mock.return_value.json = lambda: {}
    datasource.dataset = HubspotDataset.emails_events

    df = connector.get_df(datasource)

    assert df.empty


def test_hubspot_wrong_data_return(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')

    req_mock.return_value.json = lambda: {'results': 'oui'}

    try:
        connector.get_df(datasource)
    except Exception as e:
        assert isinstance(e, HubspotConnectorException)

    assert req_mock.called_once()


def test_hubspot_get_webanalytics(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')
    req_mock.return_value.json = lambda: {
        'results': [
            {
                'properties': {'foo': 42},
                'eventType': 'party',
                'occuredAt': '2021-03-08T12:28:40.305Z',
            }
        ]
    }

    datasource.object_type = HubspotObjectType.contact
    datasource.dataset = HubspotDataset.webanalytics
    datasource.parameters = {'objectProperty.email': 'foo@bar.example.com'}

    df = connector.get_df(datasource)

    assert req_mock.called_once()
    called_endpoint, kwargs = req_mock.call_args
    assert called_endpoint[0] == HUBSPOT_ENDPOINTS['web-analytics']['url']
    assert kwargs['params'] == {
        'objectType': HubspotObjectType.contact,
        'objectProperty.email': 'foo@bar.example.com',
    }
    assert not df.empty
    assert df['eventType'][0] == 'party'
    assert df['occuredAt'][0] == '2021-03-08T12:28:40.305Z'


def test_hubspot_get_email_events(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')
    req_mock.return_value.json = lambda: {
        'events': [
            {
                'appName': 'foo',
                'location': {
                    'city': 'Paris',
                    'country': 'France',
                    'state': 'Paris',
                },
                'recipient': 'bar@example.com',
                'type': 'party',
                'browser': {'name': 'chromium'},
            }
        ],
    }

    datasource.dataset = HubspotDataset.emails_events

    df = connector.get_df(datasource)

    assert req_mock.called_once()
    called_endpoint, _ = req_mock.call_args
    assert called_endpoint[0] == HUBSPOT_ENDPOINTS[HubspotDataset.emails_events]['url']
    assert not df.empty
    assert df.iloc[0]['type'] == 'party'
    assert df.iloc[0]['city'] == 'Paris'
    assert df.iloc[0]['country'] == 'France'
    assert df.iloc[0]['state'] == 'Paris'
    assert df.iloc[0]['appName'] == 'foo'
    assert df.iloc[0]['recipient'] == 'bar@example.com'
    assert df.iloc[0]['browser'] == 'chromium'


def test_hubspot_get_webanalytics_empty(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')
    req_mock.return_value.json = lambda: {}

    datasource.object_type = HubspotObjectType.contact
    datasource.dataset = HubspotDataset.webanalytics

    df = connector.get_df(datasource)

    # Check that the returned DataFrame is indeed empty through the `empty` property
    # https://pandas.pydata.org/pandas-docs/version/0.18/generated/pandas.DataFrame.empty.html
    assert df.empty


def test_hubspot_retrieve_data_pagination(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')
    requests_json_mock = mocker.Mock()
    requests_json_mock.side_effect = [
        {
            'results': [{'properties': {'foo': 42}, 'not_property': 'moo'}],
            'paging': {
                'next': {
                    'after': '42',
                    'link': 'https://api.hubapi.com/crm/v3/objects/contacts?after=42',
                }
            },
        },
        {
            'results': [{'properties': {'foo': 1337}}],
        },
    ]
    req_mock.return_value.json = requests_json_mock

    df = connector.get_df(datasource)

    assert req_mock.call_count == 2
    assert df['foo'][0] == 42
    assert df['not_property'][0] == 'moo'
    assert df['foo'][1] == 1337
    assert numpy.isnan(df['not_property'][1])


def test_hubspot_retrieve_data_pagination_legacy(connector, datasource, mocker):
    req_mock = mocker.patch('requests.get')
    requests_json_mock = mocker.Mock()
    requests_json_mock.side_effect = [
        {
            'events': [{'appId': '1337', 'appName': 'foo'}],
            'hasMore': True,
            'offset': 'baba_au_rhum42',
        },
        {
            'events': [{'appId': '42', 'appName': 'bar'}],
        },
    ]
    req_mock.return_value.json = requests_json_mock

    datasource.dataset = HubspotDataset.emails_events

    df = connector.get_df(datasource)

    assert req_mock.call_count == 2
    assert df.iloc[0]['appName'] == 'foo'
    assert df.iloc[1]['appName'] == 'bar'
