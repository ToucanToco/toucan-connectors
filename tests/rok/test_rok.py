import pytest
import responses
from jwt import encode

import datetime
from toucan_connectors.rok.rok_connector import (
    InvalidAuthenticationMethodError,
    NoROKSecretAvailableError,
    RokConnector,
    RokDataSource,
)
import mock
import json

endpoint = 'https://rok.example.com/graphql'


@pytest.fixture
def remove_secret(rok_connector_with_secret):
    rok_connector_with_secret.secret = None


@pytest.fixture
def remove_live_data_mode(rok_ds_jwt):
    rok_ds_jwt.live_data = False


@pytest.fixture
def rok_ds():
    return RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='database',
        query='{some query}',
        filter='.data',
    )


@pytest.fixture
def rok_connector():
    return RokConnector(
        name='RokConnector',
        host='https://rok.example.com',
        username='username',
        password='password',
    )


@pytest.fixture
def rok_connector_with_secret():
    return RokConnector(
        name='RokConnector',
        host='https://rok.example.com',
        username='username',
        secret='mylittlesecret',
    )


@pytest.fixture
def rok_ds_jwt():
    return RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='database',
        query='{some query}',
        filter='.data',
        live_data=True,
    )


@responses.activate

def test_rok(rok_ds, rok_connector):

    responses.add(responses.POST, endpoint, json={'data': {'authenticate': 'some_token'}})
    responses.add(responses.POST, endpoint, json={'data': {'a': 1, 'b': 2}})

    df = rok_connector.get_df(rok_ds)
    assert df['a'].sum() == 1
    assert df['b'].sum() == 2


@responses.activate
def test_rok_with_jwt(rok_connector_with_secret, rok_ds_jwt):
    """Check that we correctly retrieve the data with the crafted token"""
    # first we did a query with our jwt to get the ROK tokens
    # Here we mock ROK's response with the ROK tokens

    #Mocker retrieve with token
    #Mocker retrieve with pw
    data = {
        'token': encode(
            {'rok_token': 'rok_token'}, rok_connector_with_secret.secret, algorithm='HS256'
        ).decode('utf-8')
    }
    responses.add(responses.POST, endpoint, json={'data': data})
    # Finally we query ROK to retrieve the data with the ROK Token
    responses.add(responses.POST, endpoint, json={'data': {'a': 1, 'b': 2}})
    df = rok_connector_with_secret.get_df(rok_ds_jwt)
    assert df['a'].sum() == 1
    assert df['b'].sum() == 2

@responses.activate
def test_retrieve_token_with_jwt(rok_connector_with_secret, rok_ds_jwt, mocker):
    """check that we correctly retrieve the rok token using a jwt"""

    # This is the data returned by ROK, a token encrypted with the shared secret
    rok_token = {
        'token': encode(
            {'rok_token': 'rok_token'}, rok_connector_with_secret.secret, algorithm='HS256'
        ).decode('utf-8')
    }
    # Mocks the utcnow function for the iat field in the data we'll send
    patched = mocker.patch('toucan_connectors.rok.rok_connector.datetime')
    patched.utcnow.return_value = datetime.datetime(2020, 10, 9)

    auth_query = """
        query Auth($database: String!, $token: String!)
        {authenticateUsingJWT(database: $database, token: $token)}"""   

    payload = {
            'database': rok_ds_jwt.database,
            'username': 'username',
            'iat': datetime.datetime(2020, 10, 9),
        }
    encoded_payload = encode(payload, rok_connector_with_secret.secret, algorithm='HS256')
    auth_vars = {
                'database': rok_ds_jwt.database,
                'jwt_token': encoded_payload.decode('utf-8'),
            }
    # Mocks the response we wait from ROK JWT authentication API
    responses.add(
        method=responses.POST,
        url='http://bla.bla',
        json={'data':rok_token},

    )   
    token = rok_connector_with_secret.retrieve_token_with_jwt(
        rok_ds_jwt.database,
        endpoint='http://bla.bla')

    assert responses.assert_call_count("http://bla.bla", 1) is True
    assert json.loads(responses.calls[0].request.body) == {'query': auth_query, 'variables': auth_vars}

@responses.activate
def test_error_retrieve_token_with_jwt(rok_connector_with_secret, rok_ds_jwt, mocker):
    """Check we get an error if the ROK api replies with an error"""
    responses.add(
        method=responses.POST,
        url='http://bla.bla',
        json={'errors':'Unrecognized JWT token'})

    with pytest.raises(ValueError) as err:
        token = rok_connector_with_secret.retrieve_token_with_jwt(
        rok_ds_jwt.database,
        endpoint='http://bla.bla')

@responses.activate
def test_retrieve_token_with_password(rok_connector, rok_ds):
    """check that we correctly retrieve the rok token using a passord"""

    # This is the data returned by ROK, a token encrypted with the shared secret
    auth_query = """
        query Auth($database: String!, $user: String!, $password: String!)
        {authenticate(database: $database, user: $user, password: $password)}"""
    auth_vars = {
        'database': rok_ds.database,
        'user': rok_connector.username,
        'password': rok_connector.password,
    }
    # Mocks the response we wait from ROK password authentication API
    responses.add(
        method=responses.POST,
        url='http://bla.bla',
        json={'data':{'authenticate':'rok_token'}},
    )   
    token = rok_connector.retrieve_token_with_password(
        rok_ds.database,
        endpoint='http://bla.bla')

    assert responses.assert_call_count("http://bla.bla", 1) is True
    assert json.loads(responses.calls[0].request.body) == {'query': auth_query, 'variables': auth_vars}


@responses.activate
def test_error_retrieve_token_with_password(rok_connector, rok_ds, mocker):
    """Check we get an error if the ROK api replies with an error"""
    responses.add(
        method=responses.POST,
        url='http://bla.bla',
        json={'errors':'Wrong password'})

    with pytest.raises(ValueError) as err:
        token = rok_connector.retrieve_token_with_password(
        rok_ds.database,
        endpoint='http://bla.bla')


def test_live_data_no_secret(rok_connector_with_secret, rok_ds_jwt, remove_secret):
    """Check that we get an error as the secret is not defined"""
    with pytest.raises(NoROKSecretAvailableError):
        rok_connector_with_secret.get_df(rok_ds_jwt)


def test_token_not_live_data(rok_connector_with_secret, rok_ds_jwt, remove_live_data_mode):
    """Check that we graphqlet an error as we are using a token while not in live data mode"""
    with pytest.raises(InvalidAuthenticationMethodError):
        rok_connector_with_secret.get_df(rok_ds_jwt)


@responses.activate
def test_rok_auth_error(rok_ds, rok_connector):
    responses.add(responses.POST, endpoint, json={'errors': ['some auth error message']})

    with pytest.raises(ValueError):
        rok_connector.get_df(rok_ds)


@responses.activate
def test_rok_data_error(rok_ds, rok_connector):

    responses.add(responses.POST, endpoint, json={'data': {'authenticate': 'some_token'}})
    responses.add(responses.POST, endpoint, json={'errors': ['some data error message']})

    with pytest.raises(ValueError):
        rok_connector.get_df(rok_ds)


@pytest.mark.skip(reason='Requires a live instance')
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


@pytest.mark.skip(reason='Waiting for ROK to provide secret')
def test_live_instance_jwt(ROK_con, ROK_ds):
    import os

    ROK_con.secret = os.environ['CONNECTORS_TESTS_ROK_SECRET']
    df = ROK_con.get_df(ROK_ds)
    assert not df.empty
