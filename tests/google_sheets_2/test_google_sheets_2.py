from unittest.mock import Mock

import pytest
from pytest import fixture

import tests.general_helpers as helpers
from toucan_connectors.google_sheets_2.google_sheets_2_connector import (
    GoogleSheets2Connector,
    GoogleSheets2DataSource,
)

import_path = 'toucan_connectors.google_sheets_2.google_sheets_2_connector'


@fixture
def con():
    return GoogleSheets2Connector(name='test_name')


@fixture
def con_with_secrets(con):
    con.set_secrets({'access_token': 'foo', 'refresh_token': None})
    return con


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


def test_get_form_with_secrets(mocker, con_with_secrets, ds):
    """It should return a list of spreadsheet titles."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SHEET_LIST_RESPONSE)

    result = ds.get_form(
        connector=con_with_secrets,
        current_config={'spreadsheet_id': '1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU'},
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


def test_set_secrets(mocker, con):
    """It should set secrets on the connector."""
    spy = mocker.spy(GoogleSheets2Connector, 'set_secrets')
    fake_secrets = {
        'access_token': 'myaccesstoken',
        'refresh_token': None,
    }
    con.set_secrets(fake_secrets)

    assert con.secrets == fake_secrets
    spy.assert_called_once_with(con, fake_secrets)


def test_spreadsheet_success(mocker, con_with_secrets, ds):
    """It should return a spreadsheet."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SHEET)

    df = con_with_secrets.get_df(ds)

    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['country', 'city']

    ds.header_row = 1
    df = con_with_secrets.get_df(ds)
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ['France', 'Paris']


def test_spreadsheet_no_secrets(mocker, con, ds):
    """It should raise an exception if there no secrets passed or no access token."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SHEET)

    with pytest.raises(Exception) as err:
        con.get_df(ds)

    assert str(err.value) == 'No credentials'

    con.set_secrets({'refresh_token': None})

    with pytest.raises(KeyError):
        con.get_df(ds)


def test_set_columns(mocker, con_with_secrets, ds):
    """It should return a well-formed column set."""
    fake_results = {
        'metadata': '...',
        'values': [['Animateur', '', '', 'Week'], ['pika', '', 'a', 'W1'], ['bulbi', '', '', 'W2']],
    }
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=fake_results)

    df = con_with_secrets.get_df(ds)
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


def test_spreadsheet_without_sheet(mocker, con_with_secrets, ds_without_sheet):
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
    df = con_with_secrets.get_df(ds_without_sheet)

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
