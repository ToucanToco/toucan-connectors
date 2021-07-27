import pytest
import responses
from pytest import fixture

from toucan_connectors.oauth2_connector.oauth2connector import OAuth2Connector
from toucan_connectors.one_drive.one_drive_connector import OneDriveConnector, OneDriveDataSource

import_path = 'toucan_connectors.one_drive.one_drive_connector'


@fixture
def con(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'access_token'})
    return OneDriveConnector(
        name='test',
        auth_flow_id='test',
        client_id='CLIENT_ID',
        client_secret='CLIENT_SECRET',
        redirect_uri='REDIRECT_URI',
        secrets_keeper=secrets_keeper,
        scope='offline_access Files.Read',
    )


@fixture
def ds():
    return OneDriveDataSource(
        name='test_name',
        domain='test_domain',
        file='test_file',
        sheet='test_sheet',
        range='A2:B3',
    )


@fixture
def ds_without_range():
    return OneDriveDataSource(
        name='test_name',
        domain='test_domain',
        file='test_file',
        sheet='test_sheet',
    )


@fixture
def ds_with_site():
    return OneDriveDataSource(
        name='test_name',
        domain='test_domain',
        file='test_file',
        sheet='test_sheet',
        range='A2:B3',
        site_url='company_name.sharepoint.com/sites/site_name',
        document_library='Documents',
    )


@fixture
def ds_with_site_without_range():
    return OneDriveDataSource(
        name='test_name',
        domain='test_domain',
        file='test_file',
        sheet='test_sheet',
        site_url='company_name.sharepoint.com/sites/site_name',
        document_library='Documents',
    )


@pytest.fixture
def http_get_mock(mocker):
    return mocker.patch('requests.get')


@fixture
def remove_secrets(secrets_keeper, con):
    secrets_keeper.save('test', {'access_token': None})


FAKE_SHEET = {
    '@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#workbookRange',
    '@odata.id': '',
    '@odata.type': '#microsoft.graph.workbookRange',
    'address': 'Feuil1!A2:B3',
    'addressLocal': 'Feuil1!A2:B3',
    'cellCount': 4,
    'columnCount': 2,
    'columnHidden': False,
    'columnIndex': 0,
    'formulas': [['col1', 'col2'], ['A', 1], ['B', 2]],
    'formulasLocal': [['col1', 'col2'], ['A', 1], ['B', 2]],
    'formulasR1C1': [['col1', 'col2'], ['A', 1], ['B', 2]],
    'hidden': False,
    'numberFormat': [['General', 'General'], ['General', 'General'], ['General', 'General']],
    'rowCount': 2,
    'rowHidden': False,
    'rowIndex': 1,
    'text': [['col1', 'col2'], ['A', '1'], ['B', '2']],
    'valueTypes': [['String', 'String'], ['String', 'Double'], ['String', 'Double']],
    'values': [['col1', 'col2'], ['A', 1], ['B', 2]],
}

FAKE_LIBRARIES = {'value': [{'id': 'abcd', 'displayName': 'Documents'}]}


def test_sheet_success(mocker, con, ds, http_get_mock):
    """It should return a dataframe"""
    mocker.patch.object(OneDriveConnector, '_run_fetch', return_value=FAKE_SHEET)

    df = con.get_df(ds)

    assert http_get_mock.called_once()
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['col1', 'col2']


def test_empty_sheet(mocker, con, ds, http_get_mock):
    mocker.patch.object(OneDriveConnector, '_run_fetch', return_value={})

    df = con.get_df(ds)

    assert df.empty


def test_url_with_range(mocker, con, ds):
    mocker.patch.object(OneDriveConnector, '_run_fetch', return_value=FAKE_SHEET)

    url = con._format_url(ds)

    assert (
        url
        == "https://graph.microsoft.com/v1.0/me/drive/root:/test_file:/workbook/worksheets/test_sheet/range(address='A2:B3')"
    )


def test_url_without_range(mocker, con, ds_without_range):
    mocker.patch.object(OneDriveConnector, '_run_fetch', return_value=FAKE_SHEET)

    url = con._format_url(ds_without_range)

    assert (
        url
        == 'https://graph.microsoft.com/v1.0/me/drive/root:/test_file:/workbook/worksheets/test_sheet/usedRange(valuesOnly=true)'
    )


def test_url_with_site_with_range(mocker, con, ds_with_site):
    mocker.patch.object(OneDriveConnector, '_run_fetch', return_value=FAKE_SHEET)
    mocker.patch.object(OneDriveConnector, '_get_site_id', return_value='1234')
    mocker.patch.object(OneDriveConnector, '_get_list_id', return_value='abcd')

    url = con._format_url(ds_with_site)

    assert (
        url
        == "https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/test_file:/workbook/worksheets/test_sheet/range(address='A2:B3')"
    )


def test_url_with_site_without_range(mocker, con, ds_with_site_without_range):
    mocker.patch.object(OneDriveConnector, '_run_fetch', return_value=FAKE_SHEET)
    mocker.patch.object(OneDriveConnector, '_get_site_id', return_value='1234')
    mocker.patch.object(OneDriveConnector, '_get_list_id', return_value='abcd')

    url = con._format_url(ds_with_site_without_range)

    assert (
        url
        == 'https://graph.microsoft.com/v1.0/sites/1234/lists/abcd/drive/root:/test_file:/workbook/worksheets/test_sheet/usedRange(valuesOnly=true)'
    )


def test_build_authorization_uri(con, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    con.__dict__['_oauth2_connector'] = mock_oauth2_connector
    con.build_authorization_url()

    mock_oauth2_connector.build_authorization_url.assert_called()


def test_retrieve_tokens(con, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    con.__dict__['_oauth2_connector'] = mock_oauth2_connector
    con.retrieve_tokens('foo')

    mock_oauth2_connector.retrieve_tokens.assert_called()


def test_get_access_token(con, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    con.__dict__['_oauth2_connector'] = mock_oauth2_connector
    con._get_access_token()

    mock_oauth2_connector.get_access_token.assert_called()


def test_run_fetch(con, mocker):
    mock_oauth2_connector = mocker.Mock(spec=OAuth2Connector)
    mock_oauth2_connector.client_id = 'client_id'
    mock_oauth2_connector.client_secret = 'secret'
    con.__dict__['_oauth2_connector'] = mock_oauth2_connector

    con._run_fetch('https://jsonplaceholder.typicode.com/posts')

    mock_oauth2_connector.get_access_token.assert_called()


@responses.activate
def test_get_site_id(con, mocker, ds_with_site):
    responses.add(
        responses.GET,
        'https://graph.microsoft.com/v1.0/sites/company_name.sharepoint.com:/sites/site_name',
        json={'id': 1},
        status=200,
    )

    id = con._get_site_id(ds_with_site)
    assert id == 1


@responses.activate
def test_get_list_id(con, mocker, ds_with_site):
    responses.add(
        responses.GET, 'https://graph.microsoft.com/v1.0/sites/1234/lists', json=FAKE_LIBRARIES
    )

    id = con._get_list_id(ds_with_site, '1234')
    assert id == 'abcd'
