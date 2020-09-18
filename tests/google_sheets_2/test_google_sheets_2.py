from functools import partial
from unittest.mock import Mock

import pytest
from pytest import fixture

import tests.general_helpers as helpers
from toucan_connectors.common import HttpError
from toucan_connectors.google_sheets_2.google_sheets_2_connector import (
    GoogleSheets2Connector,
    GoogleSheets2DataSource,
    NoCredentialsError,
)

import_path = 'toucan_connectors.google_sheets_2.google_sheets_2_connector'


@fixture
def con():
    return GoogleSheets2Connector(name='test_name')


@fixture
def ds():
    return GoogleSheets2DataSource(
        name='test_name',
        domain='test_domain',
        sheet='Constants',
        spreadsheet_id='1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU',
    )


@fixture
def ds_without_sheet():
    return GoogleSheets2DataSource(
        name='test_name',
        domain='test_domain',
        spreadsheet_id='1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU',
    )


@fixture
def fake_kwargs():
    def fake_fetch_secrets(small_app_id, connector_type, auth_flow_id):
        return {'access_token': 'myaccesstoken'}

    return {'secrets': partial(fake_fetch_secrets, 'laputa', 'GoogleSheets2')}


FAKE_SHEET = {
    'metadata': '...',
    'values': [['country', 'city'], ['France', 'Paris'], ['England', 'London']],
}


@pytest.mark.asyncio
async def test_authentified_fetch(mocker, con):
    """It should return a result from fetch if all is ok."""
    mocker.patch(f'{import_path}.fetch', return_value=helpers.build_future(FAKE_SHEET))

    result = await con._authentified_fetch('/foo', 'myaccesstoken')

    assert result == FAKE_SHEET


FAKE_SHEET_LIST_RESPONSE = {
    'sheets': [
        {
            'properties': {'title': 'Foo'},
        },
        {
            'properties': {'title': 'Bar'},
        },
        {
            'properties': {'title': 'Baz'},
        },
    ]
}


def get_columns_in_schema(schema):
    """Pydantic generates schema slightly differently in python <=3.7 and in python 3.8"""
    try:
        if schema.get('definitions'):
            return schema['definitions']['sheet']['enum']
        else:
            return schema['properties']['sheet']['enum']
    except KeyError:
        return None


def test_get_form_with_secrets(mocker, con, ds, fake_kwargs):
    """It should return a list of spreadsheet titles."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SHEET_LIST_RESPONSE)

    result = ds.get_form(
        connector=con,
        current_config={'spreadsheet_id': '1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU'},
        **fake_kwargs,
    )
    expected_results = ['Foo', 'Bar', 'Baz']
    assert get_columns_in_schema(result) == expected_results


def test_get_form_no_secrets(mocker, con, ds):
    """It should return no spreadsheet titles."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=Exception)
    result = ds.get_form(
        connector=con,
        current_config={'spreadsheet_id': '1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU'},
    )
    assert not get_columns_in_schema(result)


def test_spreadsheet_success(mocker, con, ds, fake_kwargs):
    """It should return a spreadsheet."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SHEET)

    df = con.get_df(ds, **fake_kwargs)

    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['country', 'city']

    ds.header_row = 1
    df = con.get_df(ds, **fake_kwargs)
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ['France', 'Paris']


def test_spreadsheet_no_secrets(mocker, con, ds):
    """It should raise an exception if there are no secrets returned or any document in database."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SHEET)
    bogus_fake_kwargs = {'secrets': None}
    with pytest.raises(NoCredentialsError) as err:
        con.get_df(ds, **bogus_fake_kwargs)

    assert str(err.value) == 'No credentials'

    # Function that returns an empty dict, as if when no document is found in database
    def fake_fetch_secrets(small_app_id, connector_type, auth_flow_id):
        return {}

    empty_secrets_kwargs = {'secrets': partial(fake_fetch_secrets, 'laputa', 'GoogleSheets2')}

    with pytest.raises(NoCredentialsError):
        con.get_df(ds, **empty_secrets_kwargs)


def test_set_columns(mocker, con, ds, fake_kwargs):
    """It should return a well-formed column set."""
    fake_results = {
        'metadata': '...',
        'values': [['Animateur', '', '', 'Week'], ['pika', '', 'a', 'W1'], ['bulbi', '', '', 'W2']],
    }
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=fake_results)

    df = con.get_df(ds, **fake_kwargs)
    assert df.to_dict() == {
        'Animateur': {1: 'pika', 2: 'bulbi'},
        1: {1: '', 2: ''},
        2: {1: 'a', 2: ''},
        'Week': {1: 'W1', 2: 'W2'},
    }


def test__run_fetch(mocker, con):
    """It should return a result from loops if all is ok."""
    mocker.patch.object(
        GoogleSheets2Connector, '_authentified_fetch', return_value=helpers.build_future(FAKE_SHEET)
    )

    result = con._run_fetch('/fudge', 'myaccesstoken')

    assert result == FAKE_SHEET


def test_spreadsheet_without_sheet(mocker, con, ds_without_sheet, fake_kwargs):
    """
    It should retrieve the first sheet of the spreadsheet if no sheet has been indicated
    """

    def mock_api_responses(uri: str, _token):
        if uri.endswith('/Foo'):
            return FAKE_SHEET
        else:
            return FAKE_SHEET_LIST_RESPONSE

    fetch_mock: Mock = mocker.patch.object(
        GoogleSheets2Connector, '_run_fetch', side_effect=mock_api_responses
    )
    df = con.get_df(ds_without_sheet, **fake_kwargs)

    assert fetch_mock.call_count == 2
    assert (
        fetch_mock.call_args_list[0][0][0]
        == 'https://sheets.googleapis.com/v4/spreadsheets/1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU'
    )
    assert (
        fetch_mock.call_args_list[1][0][0]
        == 'https://sheets.googleapis.com/v4/spreadsheets/1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU/values/Foo'
    )

    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['country', 'city']


def test_get_status_no_secrets(mocker, con):
    """
    It should fail if no secrets are provided
    """
    assert con.get_status().status is False


def test_get_status_success(mocker, con, fake_kwargs):
    """
    It should fail if no secrets are provided.
    """
    fetch_mock: Mock = mocker.patch.object(
        GoogleSheets2Connector, '_run_fetch', return_value={'email': 'foo@bar.baz'}
    )

    connector_status = con.get_status(**fake_kwargs)
    assert connector_status.status is True
    assert 'foo@bar.baz' in connector_status.message

    fetch_mock.assert_called_once_with(
        'https://www.googleapis.com/oauth2/v2/userinfo?alt=json', 'myaccesstoken'
    )


def test_get_status_api_down(mocker, con, fake_kwargs):
    """
    It should fail if the third-party api is down.
    """
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', side_effect=HttpError)

    assert con.get_status(**fake_kwargs).status is False
