from pytest import fixture

from toucan_connectors.google_sheets_2.google_sheets_2_connector import (
    GoogleSheets2Connector,
    GoogleSheets2DataSource,
)


@fixture
def con():
    return GoogleSheets2Connector(name='test_name', access_token='qweqwe-1111-1111-1111-qweqweqwe')


@fixture
def ds():
    return GoogleSheets2DataSource(
        name='test_name',
        domain='test_domain',
        sheet='Constants',
        spreadsheet_id='1SMnhnmBm-Tup3SfhS03McCf6S4pS2xqjI6CAXSSBpHU',
    )


def test_spreadsheet(mocker, con, ds):
    pass


def test_set_columns(mocker, con, ds):
    pass
