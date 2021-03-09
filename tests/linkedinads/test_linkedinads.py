import pandas as pd
import pytest
import responses
from pytest import fixture

from toucan_connectors.common import HttpError
from toucan_connectors.linkedinads.linkedinads_connector import (
    FinderMethod,
    LinkedinadsConnector,
    LinkedinadsDataSource,
    NoCredentialsError,
    TimeGranularity,
)
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector

import_path = 'toucan_connectors.linkedinads.linkedinads_connector'


@fixture
def connector(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'coucou'})
    return LinkedinadsConnector(
        name='test',
        auth_flow_id='test',
        client_id='CLIENT_ID',
        client_secret='CLIENT_SECRET',
        redirect_uri='REDIRECT_URI',
        secrets_keeper=secrets_keeper,
    )


@fixture
def remove_secrets(secrets_keeper, connector):
    secrets_keeper.save('test', {'access_token': None})


@fixture
def create_datasource():
    return LinkedinadsDataSource(
        name='test_name',
        domain='test_domain',
        finder_methods=FinderMethod.analytics,
        start_date='01/01/2021',
        end_date='31/01/2021',
        time_granularity=TimeGranularity.all,
        flatten_column='nested',
        parameters={
            'objectiveType': 'VIDEO_VIEW',
            'campaigns': 'urn:li:sponsoredCampaign:123456,urn:li:sponsoredCampaign:654321',
        },
    )


def test_no_secrets(mocker, connector, create_datasource, remove_secrets):
    """It should raise an exception if there are no secrets returned or any document in database."""
    with pytest.raises(NoCredentialsError) as err:
        connector.get_df(create_datasource)

    assert str(err.value) == 'No credentials'


def test_get_status_no_secrets(connector, remove_secrets):
    """
    It should fail if no secrets are provided
    """
    assert connector.get_status().status is False


def test_get_status_secrets_error(mocker, connector):
    """
    It should fail if secrets can't be retrieved
    """
    mocker.patch(f'{import_path}.OAuth2Connector.get_access_token', side_effect=Exception)
    assert connector.get_status().status is False


def test_get_status_success(mocker, connector):
    mocker.patch(f'{import_path}.OAuth2Connector.get_access_token', return_value='bla')
    connector_status = connector.get_status()
    assert connector_status.status is True
    assert 'Connector status OK' in connector_status.message


@responses.activate
def test__retrieve_data(connector, create_datasource):
    responses.add(
        method='GET',
        url='https://api.linkedin.com/v2/adAnalyticsV2?',
        json={'elements': [{'bla': 'bla', 'nested': {'kikoo': 'lool'}}]},
    )
    connector.get_df(create_datasource)
    assert len(responses.calls) == 1
    assert responses.calls[0].request.headers['Authorization'] == 'Bearer coucou'
    assert (
        responses.calls[0].request.url == 'https://api.linkedin.com/v2/adAnalyticsV2?q=analytics'
        '&dateRange.start.day=1'
        '&dateRange.start.month=1'
        '&dateRange.start.year=2021'
        '&timeGranularity=ALL'
        '&dateRange.end.day=31'
        '&dateRange.end.month=1'
        '&dateRange.end.year=2021'
        '&objectiveType=VIDEO_VIEW'
        '&campaigns=urn:li:sponsoredCampaign:123456,urn:li:sponsoredCampaign:654321'
    )


@responses.activate
def test__retrieve_data_no_nested_col(connector, create_datasource):
    create_datasource.flatten_column = None
    responses.add(
        method='GET',
        url='https://api.linkedin.com/v2/adAnalyticsV2?',
        json={'elements': [{'bla': 'bla'}]},
    )
    res = connector.get_df(create_datasource)
    expected = pd.DataFrame([{'bla': 'bla'}])
    assert res['bla'][0] == expected['bla'][0]


@responses.activate
def test__retrieve_data_http_error(connector, create_datasource):
    responses.add(method='GET', url='https://api.linkedin.com/v2/adAnalyticsV2?', status=400)
    with pytest.raises(HttpError):
        connector.get_df(create_datasource)


def test_get_connectors_secrets_form(connector):
    text = connector.get_connector_secrets_form()
    assert 'Linkedin' in text.documentation_md


def test_build_authorization_url(connector):
    assert connector.build_authorization_url().startswith(
        'https://www.linkedin.com/oauth/v2/authorization?'
        'response_type=code'
        '&client_id=CLIENT_ID'
        '&redirect_uri=REDIRECT_URI'
        '&scope=r_organization_social%2Cr_ads_reporting%2Cr_ads'
    )


def test_retrieve_tokens(mocker, connector):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    connector.__dict__['_oauth2_connector'] = mock_oauth2_connector
    connector.retrieve_tokens('bla')
    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_schema_extra(create_datasource):
    conf = create_datasource.Config
    schema = {
        'properties': {
            'time_granularity': 'bar',
            'flatten_column': 'bar',
            'parameters': 'bar',
            'finder_methods': 'bar',
            'start_date': 'bar',
            'end_date': 'bar',
        }
    }
    conf.schema_extra(schema, model=LinkedinadsDataSource)

    assert schema == {
        'properties': {
            'finder_methods': 'bar',
            'start_date': 'bar',
            'end_date': 'bar',
            'time_granularity': 'bar',
            'flatten_column': 'bar',
            'parameters': 'bar',
        }
    }


@responses.activate
def test__retrieve_data_date_fallback(connector, create_datasource):
    responses.add(
        method='GET',
        url='https://api.linkedin.com/v2/adAnalyticsV2?',
        json={'elements': [{'bla': 'bla', 'nested': {'kikoo': 'lool'}}]},
    )
    ds = LinkedinadsDataSource(
        name='test_name',
        domain='test_domain',
        finder_methods=FinderMethod.analytics,
        start_date='01-01-2021',
        end_date='31-01-2021',
        time_granularity=TimeGranularity.all,
        flatten_column='nested',
        parameters={
            'objectiveType': 'VIDEO_VIEW',
            'campaigns': 'urn:li:sponsoredCampaign:123456,urn:li:sponsoredCampaign:654321',
        },
    )
    connector._retrieve_data(ds)
    assert (
        responses.calls[0].request.url == 'https://api.linkedin.com/v2/adAnalyticsV2?q=analytics'
        '&dateRange.start.day=1'
        '&dateRange.start.month=1'
        '&dateRange.start.year=2021'
        '&timeGranularity=ALL'
        '&dateRange.end.day=31'
        '&dateRange.end.month=1'
        '&dateRange.end.year=2021'
        '&objectiveType=VIDEO_VIEW'
        '&campaigns=urn:li:sponsoredCampaign:123456,urn:li:sponsoredCampaign:654321'
    )
