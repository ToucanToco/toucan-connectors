import os
from io import BytesIO

import pytest
import responses
from pytest import fixture

from toucan_connectors.net_explorer.net_explorer_connector import (
    NetExplorerConnector,
    NetExplorerDataSource,
)

# import_path = 'toucan_connectors.net_explorer.net_explorer_connector'


@fixture
def con(secrets_keeper):
    return NetExplorerConnector(
        name='test',
        instance_url='test.netexplorer.pro',
        user='test_user',
        password='test_password',
    )


@fixture
def ds_csv():
    return NetExplorerDataSource(
        name='test_name',
        domain='test_domain',
        file='bar/bax/titi.csv',
    )


@fixture
def ds_excel():
    return NetExplorerDataSource(
        name='test_name',
        domain='test_domain',
        file='bar/bax/toto.xlsx',
    )


@fixture
def ds_excel_with_sheet():
    return NetExplorerDataSource(
        name='test_name',
        domain='test_domain',
        file='bar/bax/toto.xlsx',
        sheet='Feuil2',
    )


@fixture
def ds_error_path():
    return NetExplorerDataSource(
        name='test_name',
        domain='test_domain',
        file='foo/bar/bax/toto.xlsx',
        sheet='Feuil2',
    )


FAKE_FOLDERS = [
    {'id': 1, 'name': 'foo', 'content': {'files': [], 'folders': []}},
    {
        'id': 2,
        'name': 'bar',
        'content': {
            'files': [],
            'folders': [
                {
                    'id': 3,
                    'name': 'bax',
                    'content': {
                        'folders': [],
                        'files': [{'id': 4, 'name': 'titi.csv'}, {'id': 5, 'name': 'toto.xlsx'}],
                    },
                }
            ],
        },
    },
]


@responses.activate
def test_csv(con, ds_csv):
    responses.add(responses.POST, 'https://test.netexplorer.pro/api/auth', json={'token': '123'})

    responses.add(
        responses.GET, 'https://test.netexplorer.pro/api/folders?depth=-1', json=FAKE_FOLDERS
    )

    with open(os.path.dirname(__file__) + '/fake.csv', 'rb') as file:
        fileIO = BytesIO(file.read())
        responses.add(
            responses.GET, 'https://test.netexplorer.pro/api/file/4/download', body=fileIO.read()
        )
        fileIO.seek(0)

    df = con._retrieve_data(ds_csv)
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['col1', 'col2']


@responses.activate
def test_excel(con, ds_excel):
    responses.add(responses.POST, 'https://test.netexplorer.pro/api/auth', json={'token': '123'})

    responses.add(
        responses.GET, 'https://test.netexplorer.pro/api/folders?depth=-1', json=FAKE_FOLDERS
    )

    with open(os.path.dirname(__file__) + '/fake.xlsx', 'rb') as file:
        fileIO = BytesIO(file.read())
        responses.add(
            responses.GET, 'https://test.netexplorer.pro/api/file/5/download', body=fileIO.read()
        )
        fileIO.seek(0)

    df = con._retrieve_data(ds_excel)
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ['col1', 'col2']


@responses.activate
def test_excel_with_sheet(con, ds_excel_with_sheet):
    responses.add(responses.POST, 'https://test.netexplorer.pro/api/auth', json={'token': '123'})

    responses.add(
        responses.GET, 'https://test.netexplorer.pro/api/folders?depth=-1', json=FAKE_FOLDERS
    )

    with open(os.path.dirname(__file__) + '/fake.xlsx', 'rb') as file:
        fileIO = BytesIO(file.read())
        responses.add(
            responses.GET, 'https://test.netexplorer.pro/api/file/5/download', body=fileIO.read()
        )
        fileIO.seek(0)

    df = con._retrieve_data(ds_excel_with_sheet)
    assert df.shape == (1, 1)
    assert df.columns.tolist() == ['colZ']


@responses.activate
def test_error_path(con, ds_error_path):
    responses.add(responses.POST, 'https://test.netexplorer.pro/api/auth', json={'token': '123'})

    responses.add(
        responses.GET, 'https://test.netexplorer.pro/api/folders?depth=-1', json=FAKE_FOLDERS
    )

    with open(os.path.dirname(__file__) + '/fake.xlsx', 'rb') as file:
        fileIO = BytesIO(file.read())
        responses.add(
            responses.GET, 'https://test.netexplorer.pro/api/file/5/download', body=fileIO.read()
        )
        fileIO.seek(0)

    with pytest.raises(ValueError) as e:
        con.get_df(ds_error_path)
    assert str(e.value) == 'Unable to find the file'
