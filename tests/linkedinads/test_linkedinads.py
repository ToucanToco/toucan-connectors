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
def con(secrets_keeper):
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
def remove_secrets(secrets_keeper, con):
    secrets_keeper.save('test', {'access_token': None})


@fixture
def ds():
    return LinkedinadsDataSource(
        name='test_name',
        domain='test_domain',
        finder_methods=FinderMethod.analytics,
        start_date='01/01/2021',
        end_date='31/01/2021',
        time_granularity=TimeGranularity.all,
        parameters={
            'objectiveType': 'VIDEO_VIEW',
            'campaigns': 'urn:li:sponsoredCampaign:123456,urn:li:sponsoredCampaign:654321',
        },
        filter='.',
    )


def test_no_secrets(mocker, con, ds, remove_secrets):
    """It should raise an exception if there are no secrets returned or any document in database."""
    with pytest.raises(NoCredentialsError) as err:
        con.get_df(ds)

    assert str(err.value) == 'No credentials'


def test_get_status_no_secrets(con, remove_secrets):
    """
    It should fail if no secrets are provided
    """
    assert con.get_status().status is False


def test_get_status_secrets_error(mocker, con):
    """
    It should fail if secrets can't be retrieved
    """
    mocker.patch(f'{import_path}.OAuth2Connector.get_access_token', side_effect=Exception)
    assert con.get_status().status is False


def test_get_status_success(mocker, con):
    mocker.patch(f'{import_path}.OAuth2Connector.get_access_token', return_value='bla')
    connector_status = con.get_status()
    assert connector_status.status is True
    assert 'Connector status OK' in connector_status.message


@responses.activate
def test__retrieve_data(con, ds):
    responses.add(method='GET', url='https://api.linkedin.com/v2/adAnalyticsV2?', json='blabla')
    con.get_df(ds)
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
def test__retrieve_data_http_error(con, ds):
    responses.add(method='GET', url='https://api.linkedin.com/v2/adAnalyticsV2?', status=400)
    with pytest.raises(HttpError):
        con.get_df(ds)


@responses.activate
def test__retrieve_data_jq_error(mocker, con, ds):
    responses.add(method='GET', url='https://api.linkedin.com/v2/adAnalyticsV2?', json='blabla')
    mocker.patch(f'{import_path}.transform_with_jq', side_effect=ValueError)
    mocklogger = mocker.patch(f'{import_path}.LinkedinadsConnector.logger')
    mockederror = mocklogger.error
    con.get_df(ds)
    mockederror.assert_called_once()


def test_get_connectors_secrets_form(con):
    text = con.get_connector_secrets_form()
    assert 'Linkedin' in text.documentation_md


def test_build_authorization_url(con):
    assert con.build_authorization_url().startswith(
        'https://www.linkedin.com/oauth/v2/authorization?'
        'response_type=code'
        '&client_id=CLIENT_ID'
        '&redirect_uri=REDIRECT_URI'
        '&scope=r_organization_social%2Cr_ads_reporting%2Cr_ads'
    )


def test_retrieve_tokens(mocker, con):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    con.__dict__['_oauth2_connector'] = mock_oauth2_connector
    con.retrieve_tokens('bla')
    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_schema_extra(ds):
    conf = ds.Config
    schema = {
        'properties': {
            'time_granularity': 'bar',
            'filter': 'bar',
            'parameters': 'bar',
            'finder_methods': 'bar',
            'start_date': 'bar',
            'end_date': 'bar',
        }
    }
    conf.schema_extra(schema, model='bla')

    assert schema == {
        'properties': {
            'finder_methods': 'bar',
            'start_date': 'bar',
            'end_date': 'bar',
            'time_granularity': 'bar',
            'filter': 'bar',
            'parameters': 'bar',
        }
    }
