import pytest
import responses

from toucan_connectors.rok.rok_connector import RokConnector, RokDataSource

rds = RokDataSource(
    name='RokConnector',
    domain='RokData',
    database='database',
    query='{some query}',
    filter='.data',
)

rc = RokConnector(
    name='RokConnector', host='https://rok.example.com', username='username', password='password',
)

endpoint = 'https://rok.example.com/graphql'


@responses.activate
def test_rok():

    responses.add(responses.POST, endpoint, json={'data': {'authenticate': 'some_token'}})
    responses.add(responses.POST, endpoint, json={'data': {'a': 1, 'b': 2}})

    df = rc.get_df(rds)
    assert df['a'].sum() == 1
    assert df['b'].sum() == 2


@responses.activate
def test_rok_auth_error():

    responses.add(responses.POST, endpoint, json={'errors': ['some auth error message']})

    with pytest.raises(ValueError):
        rc.get_df(rds)


@responses.activate
def test_rok_data_error():

    responses.add(responses.POST, endpoint, json={'data': {'authenticate': 'some_token'}})
    responses.add(responses.POST, endpoint, json={'errors': ['some data error message']})

    with pytest.raises(ValueError):
        rc.get_df(rds)


@pytest.mark.skip(reason='This uses a demo api')
def test_live_instance():
    import os

    live_rds = RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='ToucanToco',
        query='{entities{city{cities{code}}}}',
        filter='.data.entities.city.cities',
    )

    live_rc = RokConnector(
        name='RokConnector',
        host='https://demo.rok-solution.com',
        username=os.environ['CONNECTORS_TESTS_ROK_USERNAME'],
        password=os.environ['CONNECTORS_TESTS_ROK_PASSWORD'],
    )

    df = live_rc.get_df(live_rds)
    assert not df.empty
