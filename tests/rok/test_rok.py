import pytest
import responses
from jwt import encode, decode
from toucan_connectors.rok.rok_connector import RokConnector, RokDataSource

rds = RokDataSource(
    name='RokConnector',
    domain='RokData',
    database='database',
    query='{some query}',
    filter='.data',
)

rc = RokConnector(
    name='RokConnector',
    host='https://rok.example.com',
    username='username',
    password='password',
)

endpoint = 'https://rok.example.com/graphql'


@pytest.fixture
def ROK_con():
    return RokConnector(
        name='RokConnector',
        host='https://rok.example.com',
        username='username',
        password='password',
        secret='mylittlesecret',
    )


@pytest.fixture
def ROK_ds():
    return RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='database',
        query='{some query}',
        filter='.data',
        live_data=True,
    )


@responses.activate
def test_rok():

    responses.add(responses.POST, endpoint, json={'data': {'authenticate': 'some_token'}})
    responses.add(responses.POST, endpoint, json={'data': {'a': 1, 'b': 2}})

    df = rc.get_df(rds)
    assert df['a'].sum() == 1
    assert df['b'].sum() == 2

@responses.activate
def test_rok_with_jwt():
    """Check that we correctly retrieve the data with the crafted token"""
    # First query: we receive the ROK token
    ROK_con = RokConnector(
    name='RokConnector',
    host='https://rok.example.com',
    username='username',
    password='password',
    secret='mylittlesecret'
    )

    ROK_ds = RokDataSource(
    name='RokConnector',
    domain='RokData',
    database='database',
    query='{some query}',
    filter='.data',
    live_data = True
    )

    data = {'token':encode({'test':'data'}, ROK_con.secret, algorithm='HS256').decode('utf-8')}
    responses.add(
        responses.POST, 
        endpoint,
        json={'data':data}
        )
    # Second query: we receive the data
    responses.add(
        responses.POST, 
        endpoint, 
        json={'data':{'a':1, 'b':2}}
        )
    df = ROK_con.get_df(ROK_ds)
    assert df['a'].sum() == 1
    assert df['b'].sum() == 2



@responses.activate
def test_rok_with_jwt(ROK_con, ROK_ds):
    """Check that we correctly retrieve the data with the crafted token"""
    # First query: we receive the ROK token

    data = {
        'token': encode({'rok_token': 'rok_token'}, ROK_con.secret, algorithm='HS256').decode(
            'utf-8'
        )
    }
    responses.add(responses.POST, endpoint, json={'data': data})
    # Second query: we receive the data
    responses.add(responses.POST, endpoint, json={'data': {'a': 1, 'b': 2}})
    df = ROK_con.get_df(ROK_ds)
    assert df['a'].sum() == 1
    assert df['b'].sum() == 2


def test_live_data_no_secret():
    """Check that we get an error as the secret is not defined"""
    ROK_con = RokConnector(
        name='RokConnector',
        host='https://rok.example.com',
        username='username',
        password='password',
    )

    ROK_ds = RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='database',
        query='{some query}',
        filter='.data',
        live_data=True,
    )

    with pytest.raises(ValueError):
        ROK_con.get_df(ROK_ds)


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
