import json

import pytest
import responses

from toucan_connectors.common import transform_with_jq
from toucan_connectors.http_api.http_api_connector import Auth, HttpAPIConnector, HttpAPIDataSource


@pytest.fixture(scope='function')
def connector():
    return HttpAPIConnector(
        name='myHttpConnector', type='HttpAPI', baseroute='https://jsonplaceholder.typicode.com'
    )


@pytest.fixture(scope='function')
def data_source():
    return HttpAPIDataSource(name='myHttpDataSource', domain='my_domain', url='/comments')


@pytest.fixture(scope='function')
def auth():
    return Auth(type='basic', args=['username', 'password'])


def test_transform_with_jq():
    assert transform_with_jq(data=[1, 2, 3], jq_filter='.[]+1') == [2, 3, 4]
    assert transform_with_jq([[1, 2, 3]], '.[]') == [1, 2, 3]
    assert transform_with_jq([{'col1': [1, 2], 'col2': [3, 4]}], '.') == [
        {'col1': [1, 2], 'col2': [3, 4]}
    ]


def test_get_df(connector, data_source):
    df = connector.get_df(data_source)
    assert df.shape == (500, 5)


@responses.activate
def test_get_df_with_auth(connector, data_source, auth):
    responses.add(responses.GET, 'https://jsonplaceholder.typicode.com/comments', json=[{'a': 1}])

    connector.auth = auth
    connector.get_df(data_source)

    assert 'Authorization' in responses.calls[0].request.headers
    assert responses.calls[0].request.headers['Authorization'].startswith('Basic')


@responses.activate
def test_get_df_with_parameters(connector, data_source, mocker):
    data_source.parameters = {'first_name': 'raphael'}
    data_source.headers = {'name': '%(first_name)s'}

    responses.add(responses.GET, 'https://jsonplaceholder.typicode.com/comments', json=[{'a': 1}])

    connector.get_df(data_source)

    assert 'name' in responses.calls[0].request.headers
    assert responses.calls[0].request.headers['name'] == 'raphael'


@responses.activate
def test_get_df_with_parameters_and_auth(connector, data_source, auth, mocker):
    connector.auth = auth
    data_source.parameters = {'first_name': 'raphael'}
    data_source.headers = {'name': '%(first_name)s'}

    responses.add(responses.GET, 'https://jsonplaceholder.typicode.com/comments', json=[{'a': 1}])

    connector.get_df(data_source)

    assert 'name' in responses.calls[0].request.headers
    assert responses.calls[0].request.headers['name'] == 'raphael'


def test_exceptions_not_json():
    connector = HttpAPIConnector(
        name='myHttpConnector', type='HttpAPI', baseroute='https://demo.toucantoco.com'
    )
    data_source = HttpAPIDataSource(name='myHttpDataSource', domain='my_domain', url='/')

    with pytest.raises(ValueError):
        connector.get_df(data_source)


def test_exceptions_wrong_filter(connector, data_source):
    data_source.filter = 'bla'

    with pytest.raises(ValueError):
        connector.get_df(data_source)


def test_e2e():
    con_params = {'name': 'open_data_paris', 'baseroute': 'https://opendata.paris.fr/api/'}
    ds_params = {
        'domain': 'books',
        'name': 'open_data_paris',
        'url': 'records/1.0/search/',
        'params': {
            'dataset': 'les-1000-titres-les-plus-reserves-dans-les-bibliotheques-de-pret',
            'facet': 'auteur',
            'sort': 'rang',
            'rows': 1000,
        },
        'filter': '.records[].fields',
    }

    con = HttpAPIConnector(**con_params)
    ds = HttpAPIDataSource(**ds_params)
    df = con.get_df(ds)
    assert df.shape == (1000, 5)


@responses.activate
def test_get_df_with_json(connector, data_source, mocker):
    data_source = HttpAPIDataSource(
        name='myHttpDataSource', domain='my_domain', url='/comments', json={'a': 1}
    )

    responses.add(responses.GET, 'https://jsonplaceholder.typicode.com/comments', json=[{'a': 2}])

    connector.get_df(data_source)

    assert responses.calls[0].request.body == b'{"a": 1}'


@responses.activate
def test_get_df_with_template(data_source, mocker):
    co = HttpAPIConnector(
        **{
            'name': 'test',
            'type': 'HttpAPI',
            'baseroute': 'http://example.com',
            'template': {'headers': {'Authorization': 'XX'}},
        }
    )

    responses.add(responses.GET, 'http://example.com/comments', json=[{'a': 2}])

    co.get_df(data_source)

    h = responses.calls[0].request.headers
    assert 'Authorization' in h
    assert h['Authorization'] == co.template.headers['Authorization']


@responses.activate
def test_get_df_with_template_overide(data_source, mocker):
    co = HttpAPIConnector(
        **{
            'name': 'test',
            'type': 'HttpAPI',
            'baseroute': 'http://example.com',
            'template': {'headers': {'Authorization': 'XX', 'B': '1'}},
        }
    )

    data_source = HttpAPIDataSource(
        name='myHttpDataSource',
        domain='my_domain',
        url='/comments',
        json={'A': 1},
        headers={'Authorization': 'YY'},
    )

    responses.add(responses.GET, 'http://example.com/comments', json=[{'a': 2}])

    co.get_df(data_source)

    h = responses.calls[0].request.headers
    j = json.loads(responses.calls[0].request.body)
    assert 'Authorization' in h
    assert h['Authorization'] == data_source.headers['Authorization']
    assert 'B' in h and h['B']
    assert 'A' in j and j['A']


@pytest.mark.skip(reason='This uses an real api')
def test_get_df_oauth2_backend():

    data_provider = {
        'name': 'test',
        'type': 'HttpAPI',
        'baseroute': 'https://gateway.eu1.mindsphere.io/api/im/v3',
        'auth': {
            'type': 'oauth2_backend',
            'args': [
                'https://mscenter.piam.eu1.mindsphere.io/oauth/token',
                '<client_id>',
                '<client_secret>',
            ],
        },
    }

    users = {'domain': 'test', 'name': 'test', 'url': '/Users', 'filter': '.resources'}

    co = HttpAPIConnector(**data_provider)
    df = co.get_df(HttpAPIDataSource(**users))
    assert 'userName' in df


@responses.activate
def test_get_df_oauth2_backend_mocked():

    data_provider = {
        'name': 'test',
        'type': 'HttpAPI',
        'baseroute': 'https://gateway.eu1.mindsphere.io/api/im/v3',
        'auth': {
            'type': 'oauth2_backend',
            'args': [
                'https://mscenter.piam.eu1.mindsphere.io/oauth/token',
                '<client_id>',
                '<client_secret>',
            ],
        },
    }

    users = {'domain': 'test', 'name': 'test', 'url': '/Users'}

    responses.add(
        responses.POST,
        'https://mscenter.piam.eu1.mindsphere.io/oauth/token',
        json={'access_token': 'A'},
    )
    responses.add(
        responses.GET, 'https://gateway.eu1.mindsphere.io/api/im/v3/Users', json=[{'A': 1}]
    )

    co = HttpAPIConnector(**data_provider)
    co.get_df(HttpAPIDataSource(**users))

    assert len(responses.calls) == 2


def test_with_proxies(mocker):
    req = mocker.patch('toucan_connectors.http_api.http_api_connector.Session.request')
    f = 'toucan_connectors.http_api.http_api_connector.transform_with_jq'
    mocker.patch(f).return_value = [{'a': 1}]

    data_provider = {'name': 'test', 'type': 'HttpApi', 'baseroute': 'https://example.com'}

    data_source = {
        'proxies': {'https': 'https://eu1.proxysite.com'},
        'name': 'test',
        'domain': 'test_domain',
        'url': '/endpoint',
    }

    HttpAPIConnector(**data_provider).get_df(HttpAPIDataSource(**data_source))
    args, kwargs = req.call_args
    assert kwargs['proxies'] == {'https': 'https://eu1.proxysite.com'}


def test_with_cert(mocker):
    req = mocker.patch('toucan_connectors.http_api.http_api_connector.Session.request')
    f = 'toucan_connectors.http_api.http_api_connector.transform_with_jq'
    mocker.patch(f).return_value = [{'a': 1}]

    data_provider = {
        'name': 'test',
        'type': 'HttpApi',
        'baseroute': 'https://example.com',
        'cert': ['tests/http_api/test_http_api.py', 'tests/http_api/test_http_api.py'],
    }

    data_source = {'name': 'test', 'domain': 'test_domain', 'url': '/endpoint'}

    HttpAPIConnector(**data_provider).get_df(HttpAPIDataSource(**data_source))
    args, kwargs = req.call_args
    assert kwargs['cert'] == ['tests/http_api/test_http_api.py', 'tests/http_api/test_http_api.py']
