import json
import os

import pandas as pd
import pytest
import responses
from responses import matchers

from toucan_connectors.anaplan.anaplan_connector import AnaplanConnector, AnaplanDataSource


@pytest.fixture()
def anaplan_auth_response() -> dict:
    return {'tokenInfo': {'tokenValue': 'SomethingNotEntirelySecret'}}


@pytest.fixture()
def connector() -> AnaplanConnector:
    return AnaplanConnector(username='JohnDoe', password='s3cr3t', name="John's connector")


@responses.activate
def test_get_status_expect_auth_ok(connector):
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        json={'tokenInfo': {'tokenValue': 'youpi'}},
        status=200,
    )
    status = connector.get_status()
    assert status.status
    assert status.error is None


@responses.activate
def test_get_status_expect_auth_failed_http_40X(connector):
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        status=401,
    )

    status = connector.get_status()
    assert not status.status
    assert 'credentials' in status.error


@responses.activate
def test_get_status_expect_auth_failed_invalid_resp_body(connector):
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        status=200,
        json={'nein': 'nope'},
    )

    status = connector.get_status()
    assert not status.status
    assert 'nein' in status.error


@responses.activate
def test_get_form(connector):
    # response format taken from
    # https://anaplanbulkapi20.docs.apiary.io/#Models
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        json={'tokenInfo': {'tokenValue': 'youpi'}},
        status=200,
    )

    responses.add(
        responses.GET,
        'https://api.anaplan.com/2/0/workspaces',
        status=200,
        json={
            'meta': {},
            'status': {},
            'workspaces': [
                {'id': 'w1', 'active': True, 'name': 'Workspace One', 'sizeAllowance': 1234}
            ],
        },
    )

    responses.add(
        responses.GET,
        'https://api.anaplan.com/2/0/workspaces/w1/models',
        status=200,
        json={
            # NOTE: There's paging information in here, we
            # should think about checking if we retrieved all pages
            'meta': {},
            # HTTP status info. Are the returned HTTP status
            # codes consistent with what's in here ?
            'status': {},
            'models': [
                {
                    'id': 'm1',
                    'activeState': 'UNLOCKED',
                    'name': 'Model One',
                    'currentWorkspaceId': 'w1',
                    'currentWorkspaceName': 'NiceWorkspace',
                    'categoryValues': [],
                }
            ],
        },
    )

    responses.add(
        responses.GET,
        'https://api.anaplan.com/2/0/workspaces/w1/models/m1/views',
        status=200,
        json={
            'views': [
                {'name': 'View One', 'id': 'm1v1'},
                {'name': 'View Two', 'id': 'm1v2'},
            ]
        },
    )

    form_schema = AnaplanDataSource.get_form(
        connector, {'model_id': 'm1', 'workspace_id': 'w1 - Workspace One'}
    )

    breakpoint()
    # Ensure we've only requested a token once
    responses.assert_call_count('https://auth.anaplan.com/token/authenticate', 1)
    assert form_schema['definitions']['workspace_id']['enum'] == ['w1 - Workspace One']
    assert form_schema['definitions']['model_id']['enum'] == ['m1 - Model One']
    assert form_schema['definitions']['view_id']['enum'] == ['m1v1 - View One', 'm1v2 - View Two']


@responses.activate
def test_get_df(connector):
    responses.add(
        responses.POST,
        'https://auth.anaplan.com/token/authenticate',
        json={'tokenInfo': {'tokenValue': 'youpi'}},
        status=200,
    )

    # response format taken from
    # https://anaplanbulkapi20.docs.apiary.io/#RetrieveCellDataView
    with open(
        os.path.join(os.path.dirname(__file__), 'fixtures/cell-data-view.json')
    ) as fixture_file:
        responses.add(
            responses.GET,
            'https://api.anaplan.com/2/0/models/m1/views/m1v1/data?format=v1',
            status=200,
            match=[
                matchers.header_matcher(
                    {'Accept': 'application/json', 'Authorization': 'AnaplanAuthToken youpi'}
                )
            ],
            json=json.load(fixture_file),
        )

    df = connector.get_df(
        AnaplanDataSource(
            name='anaplan_test_api',
            domain='data_for_m1v1',
            model_id='m1',
            view_id='m1v1',
            workspace_id='w1',
        )
    )

    assert isinstance(df, pd.DataFrame)
    assert df.index.to_list() == ['Durham', 'Newcastle upon Tyne', 'Sunderland']
    assert df.columns.to_list() == [
        'Jan 13',
        'Feb 13',
        'Mar 13',
        'Q1 FY13',
        'Apr 13',
        'May 13',
        'Jun 13',
        'Q2 FY13',
        'H1 FY13',
        'Jul 13',
        'Aug 13',
        'Sep 13',
        'Q3 FY13',
        'Oct 13',
        'Nov 13',
        'Dec 13',
        'Q4 FY13',
        'H2 FY13',
        'FY13',
    ]
