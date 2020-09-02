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
def ds():
    return GoogleSheets2DataSource(
        name='test_name',
        domain='test_domain',
        sheet='Constants',
        spreadsheet_id='1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU',
    )


FAKE_SPREADSHEET = {
    'metadata': '...',
    'values': [['country', 'city'], ['France', 'Paris'], ['England', 'London']],
}


@pytest.mark.asyncio
async def test_get_data(mocker, con):
    """It should return a result from fetch if all is ok."""
    mocker.patch(f'{import_path}.fetch', return_value=helpers.build_future(FAKE_SPREADSHEET))

    result = await con._get_data('/foo', 'myaccesstoken')

    assert result == FAKE_SPREADSHEET


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


def test_get_form_with_secrets(mocker, con, ds):
    """It should return a list of spreadsheet titles."""
    con.set_secrets({'access_token': 'foo'})
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SHEET_LIST_RESPONSE)

    result = ds.get_form(
        connector=con,
        current_config={'spreadsheet_id': '1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU'},
    )
    expected_results = ['Foo', 'Bar', 'Baz']
    if result.get('definitions'):
        assert result['definitions']['sheet']['enum'] == expected_results
    else:
        assert result['properties']['sheet']['enum'] == expected_results


def test_get_form_no_secrets(mocker, con, ds):
    """It should return no spreadsheet titles."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=Exception)
    result = ds.get_form(
        connector=con,
        current_config={'spreadsheet_id': '1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU'},
    )
    if result.get('properties'):
        assert not result['properties']['sheet'].get('enum')
    else:
        assert not result.get('definitions')


def test_set_secrets(mocker, con):
    """It should set secrets on the connector."""
    spy = mocker.spy(GoogleSheets2Connector, 'set_secrets')
    fake_secrets = {
        'access_token': 'myaccesstoken',
        'refresh_token': 'myrefreshtoken',
    }
    con.set_secrets(fake_secrets)

    assert con.secrets == fake_secrets
    spy.assert_called_once_with(con, fake_secrets)


def test_spreadsheet_success(mocker, con, ds):
    """It should return a spreadsheet."""
    con.set_secrets(
        {
            'access_token': 'myaccesstoken',
            'refresh_token': 'myrefreshtoken',
        }
    )

    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SPREADSHEET)

    df = con.get_df(ds)

    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['country', 'city']

    ds.header_row = 1
    df = con.get_df(ds)
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ['France', 'Paris']


def test_spreadsheet_no_secrets(mocker, con, ds):
    """It should raise an exception if there no secrets passed or no access token."""
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=FAKE_SPREADSHEET)

    with pytest.raises(Exception) as err:
        con.get_df(ds)

    assert str(err.value) == 'No credentials'

    con.set_secrets({'refresh_token': 'myrefreshtoken'})

    with pytest.raises(KeyError):
        con.get_df(ds)


def test_set_columns(mocker, con, ds):
    """It should return a well-formed column set."""
    con.set_secrets({'access_token': 'foo', 'refresh_token': 'bar'})
    fake_results = {
        'metadata': '...',
        'values': [['Animateur', '', '', 'Week'], ['pika', '', 'a', 'W1'], ['bulbi', '', '', 'W2']],
    }
    mocker.patch.object(GoogleSheets2Connector, '_run_fetch', return_value=fake_results)

    df = con.get_df(ds)
    assert df.to_dict() == {
        'Animateur': {1: 'pika', 2: 'bulbi'},
        1: {1: '', 2: ''},
        2: {1: 'a', 2: ''},
        'Week': {1: 'W1', 2: 'W2'},
    }


@pytest.mark.skip(reason='Update seems to have broken this test')
def test__run_fetch(mocker, con):
    """It should return a result from loops if all is ok."""
    mocker.patch.object(
        GoogleSheets2Connector, '_get_data', return_value=helpers.build_future(FAKE_SPREADSHEET)
    )

    result = con._run_fetch('/fudge', 'myaccesstoken')

    assert result == FAKE_SPREADSHEET
