import pytest
import responses

from toucan_connectors.anaplan.anaplan_connector import AnaplanConnector, AnaplanDataSource


@pytest.fixture()
def anaplan_auth_response() -> dict:
    return {'tokenInfo': {'tokenValue': 'SomethingNotEntirelySecret'}}


@pytest.fixture()
def connector() -> AnaplanConnector:
    return AnaplanConnector(
        username='JohnDoe', password='s3cr3t', name="John's connector", workspace_id='w1'
    )


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
        'https://api.anaplan.com/2/0/models',
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
                    'name': 'ModelOne',
                    'currentWorkspaceId': 'w1',
                    'currentWorkspaceName': 'NiceWorkspace',
                    'categoryValues': [],
                }
            ],
        },
    )

    responses.add(
        responses.GET,
        'https://api.anaplan.com/2/0/models/m1/views',
        status=200,
        json={
            'views': [
                {'name': 'ViewOne', 'id': 'm1v1'},
                {'name': 'ViewTwo', 'id': 'm1v2'},
            ]
        },
    )

    form_schema = AnaplanDataSource.get_form(connector, {'model_id': 'm1'})

    assert form_schema['definitions']['model_id']['enum'] == ['m1']
    assert form_schema['definitions']['view_id']['enum'] == ['m1v1', 'm1v2']
