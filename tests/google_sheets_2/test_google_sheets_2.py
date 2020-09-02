from pytest import fixture

from toucan_connectors.google_sheets_2.google_sheets_2_connector import (
    GoogleSheets2Connector,
    GoogleSheets2DataSource,
)


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


def test__set_secrets(mocker, con):
    """It should set secrets on the connector."""
    spy = mocker.spy(GoogleSheets2Connector, 'set_secrets')
    fake_secrets = {
        'access_token': 'myaccesstoken',
        'refresh_token': 'myrefreshtoken',
    }
    con.set_secrets(fake_secrets)

    assert con.secrets == fake_secrets
    spy.assert_called_once_with(con, fake_secrets)


def test_retrieve_data(con, ds):
    """It should just work for now."""
    con._retrieve_data(ds)
