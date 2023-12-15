from unittest import mock

import pandas as pd
import responses
from pytest import fixture
from pytest_mock import MockerFixture

from toucan_connectors.common import HttpError
from toucan_connectors.google_adwords.google_adwords_connector import (
    GoogleAdwordsConnector,
    GoogleAdwordsDataSource,
)
from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector

import_path = 'toucan_connectors.google_adwords.google_adwords_connector'


@fixture
def connector(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'access_token', 'refresh_token': 'refresh_token'})
    return GoogleAdwordsConnector(
        name='test',
        auth_flow_id='test',
        client_id='CLIENT_ID',
        client_secret='CLIENT_SECRET',
        redirect_uri='REDIRECT_URI',
        secrets_keeper=secrets_keeper,
        developer_token='nekot_repoleved',
        client_customer_id='nice_customer',
    )


@fixture
def remove_secrets(secrets_keeper, connector):
    secrets_keeper.save('test', {'access_token': None})


@fixture
def build_data_service_source():
    return GoogleAdwordsDataSource(
        name='test_name',
        domain='test_domain',
        service='CampaignService',
        columns='Id, Name, Status',
        parameters={'Status': {'operator': 'EqualTo', 'value': 'ENABLED'}},
        orderby={'column': 'Name', 'direction': 'Asc'},
    )


@fixture
def build_report_data_source():
    return GoogleAdwordsDataSource(
        name='test_name',
        domain='test_domain',
        from_clause='CriteriaReport',
        columns='Id, Adname',
        parameters={'Adname': {'operator': 'StartsWith', 'value': 'G'}},
        during='20210101, 20210131',
    )


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


@responses.activate
def test_get_status_success(mocker, connector):
    """
    It should fail if no secrets are provided.
    """
    responses.add(
        'GET',
        url='https://www.googleapis.com/oauth2/v2/userinfo?alt=json',
        json={'email': 'kikoolol'},
    )

    connector_status = connector.get_status()
    assert responses.calls[0].request.headers['Authorization'] == 'Bearer access_token'
    assert 'kikoolol' in connector_status.message


@responses.activate
def test_get_status_api_down(mocker, connector):
    """
    It should fail if the third-party api is down.
    """
    mockreq = mocker.patch(f'{import_path}.requests')
    mockehttperror = mockreq.get
    mockehttperror.side_effect = HttpError

    assert connector.get_status().status is False


def test_build_authorization_url(connector):
    assert connector.build_authorization_url().startswith(
        'https://accounts.google.com/o/oauth2/auth?'
        'access_type=offline'
        '&prompt=consent'
        '&response_type=code'
        '&client_id=CLIENT_ID'
        '&redirect_uri=REDIRECT_URI'
        '&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fadwords&state='
    )


def test_retrieve_tokens(mocker, connector):
    """
    Check that the retrieve_tokens method properly returns
    tokens
    """
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'test_client_id'
    mock_oauth2_connector.client_secret = 'test_client_secret'
    connector._oauth2_connector = mock_oauth2_connector
    connector.retrieve_tokens('bla')
    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_model_json_schema(build_data_service_source: GoogleAdwordsDataSource):
    """
    Check that schema_extra correctly
    structures the Data Source form
    """
    assert list(build_data_service_source.model_json_schema()['properties'].keys())[:7] == [
        'service',
        'columns',
        'from_clause',
        'parameters',
        'during',
        'orderby',
        'limit',
    ]


def test_get_connectors_secrets_form(connector):
    """
    Check that get_connector_secrets_form correctly
    retrieves the credentials configuration doc
    """
    text = connector.get_connector_secrets_form()
    assert 'Adwords' in text.documentation_md


def test_get_refresh_token(connector, mocker):
    """
    Check that get_refresh_token calls
    the oAuth2 connector get_refresh_token method
    """
    mocked_refresh = mocker.patch(f'{import_path}.OAuth2Connector.get_refresh_token')
    connector.get_refresh_token()
    mocked_refresh.assert_called_once()


def test_authenticate_client(connector, mocker):
    """
    Check that authenticate_client is able to configure
    & return an AdWordsClient
    """
    mocked_google_oauth2 = mocker.patch(
        f'{import_path}.oauth2.GoogleRefreshTokenClient', return_value='fake_oauth2_client'
    )
    mocked_adwords_client = mocker.patch(f'{import_path}.AdWordsClient')
    mocked_refresh = mocker.patch(f'{import_path}.OAuth2Connector.get_refresh_token')
    connector.authenticate_client()
    mocked_google_oauth2.assert_called_once()
    mocked_adwords_client.assert_called_once()
    mocked_refresh.assert_called_once()


def test_prepare_service_query(
    connector: GoogleAdwordsConnector,
    mocker: MockerFixture,
    build_data_service_source: GoogleAdwordsDataSource,
):
    """
    Check that prepare_service_query is able to build
    & return a service and a built service query
    """
    mocked_adwords_client = mocker.patch(f'{import_path}.AdWordsClient')
    mock_get_service = mocker.patch('googleads.common.GetServiceClassForLibrary')
    impl = mock.Mock()
    mock_service = mock.Mock()
    impl.return_value = mock_service
    mock_get_service.return_value = impl
    mock_apply_filter = mocker.patch(
        f'{import_path}.apply_filter',
    )
    _, built_service_query = connector.prepare_service_query(
        client=mocked_adwords_client, data_source=build_data_service_source
    )
    mock_apply_filter.assert_called_once()
    assert built_service_query._awql == 'SELECT Id, Name, Status ORDER BY Name ASC'


def test_prepare_report_query(connector, mocker, build_report_data_source):
    """
    Check that prepare_service_query is able to build
    & return a service and a built service query
    """
    mocked_adwords_client = mocker.patch(f'{import_path}.AdWordsClient')
    mock_get_report_downloader = mocked_adwords_client.GetReportDownloader
    mock_get_report_downloader.return_value = mock.Mock()
    mock_apply_filter = mocker.patch(
        f'{import_path}.apply_filter',
    )
    _, built_report_query = connector.prepare_report_query(
        client=mocked_adwords_client, data_source=build_report_data_source
    )
    mock_apply_filter.assert_called_once()
    assert built_report_query._awql == 'SELECT Id, Adname FROM CriteriaReport DURING 20210101, 20210131'


def test__retrieve_data_service(connector, build_data_service_source, mocker):
    """
    Check that _retrieve_data with a service data source does the correct calls
    """
    mocker.patch(f'{import_path}.GoogleAdwordsConnector.authenticate_client')

    def return_pager_content(arg):
        return [{'entries': [{'Id': 1, 'Name': 'a', 'Status': 'ENABLED'}]}]

    fake_query = mock.Mock(Pager=return_pager_content)
    mocked_built_query = mocker.patch(
        f'{import_path}.GoogleAdwordsConnector.prepare_service_query',
        return_value=('foo', fake_query),
    )
    mocked_built_query.Pager()
    mocker.patch(f'{import_path}.clean_columns', return_value=['id', 'name', 'status'])
    mocker.patch(f'{import_path}.serialize_object', return_value={'id': 1, 'name': 'a', 'status': 'ENABLED'})
    res = connector._retrieve_data(build_data_service_source)
    assert res['id'][0] == 1
    mocked_built_query.assert_called_once()


def test__retrieve_data_report(connector, build_report_data_source, mocker):
    """
    Check that _retrieve_data with a service data source does the correct calls
    """
    mocker.patch(f'{import_path}.GoogleAdwordsConnector.authenticate_client')

    fake_query = 'SELECT Id, AdName FROM CriteriaReport WHERE AdName StartsWith "G" DURING 20210101,20210131'
    mocked_report_downloader = mock.Mock()
    mocked_report_downloader.DownloadReportWithAwql().return_value = [
        {'Id': 1, 'AdName': 'a'},
        {'Id': 2, 'AdName': 'b'},
    ]
    mocked_prepare = mocker.patch(
        f'{import_path}.GoogleAdwordsConnector.prepare_report_query',
        return_value=(mocked_report_downloader, fake_query),
    )
    mocked_io = mocker.patch(f'{import_path}.StringIO')
    mocked_pandas = mocker.patch(f'{import_path}.pd')
    mocked_pandas.read_csv.return_value = pd.DataFrame([{'id': 1, 'adName': 'a'}, {'id': 2, 'adName': 'b'}])
    connector._retrieve_data(build_report_data_source)
    assert (
        mocked_report_downloader.DownloadReportWithAwql.call_args_list[1][0][0]
        == 'SELECT Id, AdName FROM CriteriaReport WHERE AdName StartsWith "G" DURING 20210101,20210131'
    )
    mocked_prepare.assert_called_once()
    mocked_io.assert_called_once()
