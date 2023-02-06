import pytest
import responses

from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.rok.rok_connector import (
    InvalidAuthenticationMethodError,
    InvalidJWTError,
    InvalidUsernameError,
    NoROKSecretAvailableError,
    RokConnector,
    RokDataSource,
)

endpoint = 'https://rok.example.com/graphql'


@pytest.fixture
def remove_secret(rok_connector_with_secret):
    rok_connector_with_secret.secret = None


@pytest.fixture
def activate_authentication_with_token(rok_connector_with_secret):
    rok_connector_with_secret.authenticated_with_token = True


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
        secret='mylittlesecret==',  # base64 encoded
        authenticated_with_token=True,
    )


@pytest.fixture
def rok_ds_jwt():
    return RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='database',
        query='{some query}',
        filter='.data.entities.company.companies',
        live_data=True,
    )


@responses.activate
def test_rok_with_password(rok_ds, rok_connector):
    responses.add(
        responses.POST,
        f'{endpoint}?DatabaseName={rok_ds.database}',
        json={'data': {'authenticate': 'some_token'}},
    )
    responses.add(
        responses.POST,
        f'{endpoint}?DatabaseName={rok_ds.database}',
        json={'data': {'a': 1, 'b': 2}},
    )

    df = rok_connector.get_df(rok_ds)
    assert df['a'].sum() == 1
    assert df['b'].sum() == 2


@responses.activate
def test_rok_with_jwt(rok_connector_with_secret, rok_ds_jwt, activate_authentication_with_token):
    """Check that we correctly retrieve the data with the crafted token"""
    # if our JWT is correctly crafted then ROK will reply with the data related to the provided query

    responses.add(
        responses.POST,
        endpoint,
        json={'data': {'entities': {'company': {'companies': [{'code': 'aaa'}, {'code': 'bbb'}]}}}},
    )
    df = rok_connector_with_secret.get_df(rok_ds_jwt)
    assert df['code'][0] == 'aaa'
    assert df['code'][1] == 'bbb'


@responses.activate
def test_wrong_jwt(rok_connector_with_secret, rok_ds_jwt):
    """Check that an exception is raised if the jwt is not validated by ROK"""
    responses.add(method=responses.POST, url='http://bla.bla', body='<Fault xmlns= ...')

    with pytest.raises(InvalidJWTError):
        rok_connector_with_secret.retrieve_data_with_jwt(rok_ds_jwt, endpoint='http://bla.bla')


@responses.activate
def test_wrong_query(rok_connector_with_secret, rok_ds_jwt):
    """Check that we have an error when using an invalid query"""
    responses.add(responses.POST, endpoint, json={'errors': ['some data error message']})

    with pytest.raises(ValueError):
        rok_connector_with_secret.get_df(rok_ds_jwt)


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
        json={'data': {'authenticate': 'rok_token'}},
    )
    rok_connector.retrieve_token_with_password(rok_ds.database, endpoint='http://bla.bla')

    assert responses.assert_call_count('http://bla.bla', 1) is True
    assert JsonWrapper.loads(responses.calls[0].request.body) == {
        'query': auth_query,
        'variables': auth_vars,
    }


@responses.activate
def test_error_retrieve_token_with_password(rok_connector, rok_ds):
    """Check we get an error if the ROK api replies with an error"""
    responses.add(method=responses.POST, url='http://bla.bla', json={'errors': 'Wrong password'})

    with pytest.raises(ValueError):
        rok_connector.retrieve_token_with_password(rok_ds.database, endpoint='http://bla.bla')


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


@responses.activate
def test_wrong_username(rok_ds_jwt, rok_connector_with_secret):
    """
    check that an error is triggered when the connector
    tries to authenticate with a wrong username
    """
    responses.add(
        responses.POST,
        'https://rok.example.com/graphql',
        json={
            'Message': 'JwtLogon: The user is not authenticated!',
            'StackTrace': '',
            'ErrorCode': 1,
            'ExceptionCategory': 1,
            'ExceptionParamsFormat': None,
        },
    )
    with pytest.raises(InvalidUsernameError):
        rok_connector_with_secret.get_df(rok_ds_jwt)


@responses.activate
def test_error_message_from_rok(rok_ds_jwt, rok_connector_with_secret):
    """
    Check that an error is triggered if we have an error response from ROK
    """
    responses.add(
        responses.POST,
        'https://rok.example.com/graphql',
        json={
            'Message': 'Random Error message',
            'StackTrace': '',
            'ErrorCode': 1,
            'ExceptionCategory': 1,
            'ExceptionParamsFormat': None,
        },
    )
    with pytest.raises(ValueError):
        rok_connector_with_secret.get_df(rok_ds_jwt)


@pytest.mark.skip(reason='Requires live instance wih username/password Authentication')
def test_live_instance():
    live_rds = RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='ToucanToco',
        query='{entities{city{cities{code}}}}',
        filter='.data.entities.city.cities',
    )

    live_rc = RokConnector(
        name='RokConnector',
        host='',
        username='',
        password='',
    )

    df = live_rc.get_df(live_rds)
    print(df)
    assert len(df.values) > 1


@pytest.mark.skip(reason='Requires live instance wih JWT Authentication')
def test_live_instance_jwt():
    """
    Check that we are able to retrieve data from ROK's demo instance
    with a ROK Connector authenticated with a JWT
    """

    live_rc_jwt = RokConnector(
        name='RokConnector',
        host='',
        username='',
        secret='',
        authenticated_with_token=True,
    )

    live_rds = RokDataSource(
        name='RokConnector',
        domain='RokData',
        database='ToucanToco',
        query='{entities{company{companies{code}}}}',
        filter='.data.entities.company.companies',
        live_data=True,
    )

    df = live_rc_jwt.get_df(live_rds)
    assert len(df.values) > 1


@pytest.mark.parametrize(
    'data_source_params, data_source_date_viewid, query, expected',
    [
        (
            None,
            {'start_date': '10/07/2019', 'end_date': '10/08/2020', 'viewId': '3333'},
            '{cartographies{dataTracking(startUtcDate:"%(start_date)s", endUtcDate:"%(end_date)s", viewId:"%(viewId)s"{procedureInstancesWithRuleReportings{procedureInstance{id shortDescriptionSetter}}}}}',
            ['10/07/2019'],
        ),
        (
            {'foo': 'bar'},
            {'start_date': None, 'end_date': None, 'viewId': None},
            '{cartographies{dataTracking(startUtcDate:"%(start_date)s", endUtcDate:"%(end_date)s", foo: "%(foo)s", viewId:"%(viewId)s"{procedureInstancesWithRuleReportings{procedureInstance{id shortDescriptionSetter}}}}}',
            ['None', 'bar'],
        ),
        (
            {'foo': 'bar'},
            {'start_date': None, 'end_date': None, 'viewId': None},
            '{cartographies{dataTracking(startUtcDate:"0", endUtcDate:"1", foo: "2", viewId:"3"{procedureInstancesWithRuleReportings{procedureInstance{id shortDescriptionSetter}}}}}',
            ['1'],
        ),
    ],
)
@responses.activate
def test_interpolate_parameters(
    rok_connector, rok_ds, mocker, data_source_params, data_source_date_viewid, query, expected
):
    """
    check that the query is correctly built with interpolated variables
    first case: parameters in the form are set, no parameters in data_source json and placeholders are available in the query
    second case: parameters in the form are not set, a parameter exists in data_source json and placeholders are defined in the query
    third case: parameters in the form are not set, a parameter exists in data_source json but no placeholder defined in the query
    If the conceptor tries a query with placeholders but no matching parameters it will fail, but he is aware of this when using variables in query
    """
    mocker.patch.object(
        RokConnector, 'retrieve_token_with_password', return_value='fake_authentication_token'
    )

    responses.add(
        method=responses.POST,
        url='https://rok.example.com/graphql?DatabaseName=database',
        json={'foo': 'bar'},
    )

    rok_ds.query = query
    rok_ds.start_date = data_source_date_viewid['start_date']
    rok_ds.end_date = data_source_date_viewid['end_date']
    rok_ds.viewId = data_source_date_viewid['viewId']
    rok_ds.parameters = data_source_params
    rok_connector.get_df(rok_ds)

    for e in expected:
        assert e in JsonWrapper.loads(responses.calls[0].request.body)['query']
