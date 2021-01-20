import pytest

from toucan_connectors.salesforce.salesforce_connector import (
    SalesforceConnector,
    SalesforceDataSource,
)


@pytest.fixture
def sc(secrets_keeper):
    secrets_keeper.save('test', {'access_token': 'access_token'})
    return SalesforceConnector(
        name='test',
        auth_flow_id='test',
        client_id='test_client_id',
        client_secret='test_client_secret',
        secrets_keeper=secrets_keeper,
        redirect_uri='https://redirect.me/',
        instance_url='https://salesforce.is.awsome',
    )


@pytest.fixture
def remove_secrets(secrets_keeper, sc):
    secrets_keeper.save('test', {'access_token': None})


@pytest.fixture(scope='session')
def ds():
    return SalesforceDataSource(
        name='sfds',
        domain='sfd',
        query='select name from magictable',
    )


@pytest.fixture(scope='session')
def toys_results_p1():
    return {
        'totalSize': 2,
        'done': True,
        'records': [
            {
                'attributes': {'type': 'Toy', 'url': '/services/data/v39.0/sobjects/Toy/A111FA'},
                'Id': 'A111FA',
                'Name': 'Magic Poney',
            },
            {
                'attributes': {'type': 'Toy', 'url': '/services/data/v39.0/sobjects/Toy/A111FB'},
                'Id': 'A111FB',
                'Name': 'Wonderful Panther',
            },
        ],
        'nextRecordsUrl': 'comehere!',
    }


@pytest.fixture(scope='session')
def toys_results_p2():
    return {
        'totalSize': 1,
        'done': True,
        'records': [
            {
                'attributes': {'type': 'Toy', 'url': '/services/data/v39.0/sobjects/Toy/A111FC'},
                'Id': 'A111FC',
                'Name': 'Lightling Lizard',
            },
        ],
    }


@pytest.fixture(scope='session')
def error_result():
    return [
        {
            'message': 'oh shoe you did something wrong',
            'errorCode': 'we have a wonderful doc to help you ... nah just kidding :)',
        }
    ]


@pytest.fixture(scope='session')
def clean_p1():
    return [
        {'Id': 'A111FA', 'Name': 'Magic Poney'},
        {'Id': 'A111FB', 'Name': 'Wonderful Panther'},
        {'Id': 'A111FC', 'Name': 'Lightling Lizard'},
    ]
