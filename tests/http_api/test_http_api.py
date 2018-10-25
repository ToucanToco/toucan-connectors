import pytest

from toucan_connectors.http_api.http_api_connector import (
    HttpAPIConnector,
    HttpAPIDataSource,
    transform_with_jq,
    Auth,
    HTTPBasicAuth
)


@pytest.fixture(scope='function')
def connector():
    return HttpAPIConnector(name="myHttpConnector", type="HttpAPI",
                            baseroute="https://jsonplaceholder.typicode.com")


@pytest.fixture(scope='function')
def data_source():
    return HttpAPIDataSource(name="myHttpDataSource", domain="my_domain", url="/comments")


@pytest.fixture(scope='function')
def auth():
    return Auth(type="basic", args=["username", "password"])


def test_transform_with_jq():
    assert transform_with_jq(data=[1, 2, 3], jq_filter='.[]+1') == [2, 3, 4]
    assert transform_with_jq([[1, 2, 3]], '.[]') == [1, 2, 3]
    assert transform_with_jq(
        [{'col1': [1, 2], 'col2': [3, 4]}], '.') == [{'col1': [1, 2], 'col2': [3, 4]}]


def test_get_df(connector, data_source):
    df = connector.get_df(data_source)
    assert df.shape == (500, 5)


def test_get_df_with_auth(connector, data_source, auth, mocker):
    connector.auth = auth
    mocke = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mocke.return_value.json.return_value = []

    connector.get_df(data_source)
    _, ke = mocke.call_args

    assert ke["auth"].username == 'username'
    assert isinstance(ke["auth"], HTTPBasicAuth)


def test_get_df_with_parameters(connector, data_source, mocker):
    data_source.parameters = {"first_name": "raphael"}
    data_source.headers = {"name": "%(first_name)s"}

    mock = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mock.return_value.json.return_value = []

    connector.get_df(data_source)
    _, ke = mock.call_args

    assert ke["headers"] == {"name": "raphael"}


def test_get_df_with_parameters_and_auth(connector, data_source, auth, mocker):
    connector.auth = auth
    data_source.parameters = {"first_name": "raphael"}
    data_source.headers = {"name": "%(first_name)s"}

    mock = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mock.return_value.json.return_value = []

    connector.get_df(data_source)
    _, ke = mock.call_args

    assert ke["headers"] == {"name": "raphael"}


def test_exceptions_not_json():
    connector = HttpAPIConnector(name="myHttpConnector", type="HttpAPI",
                                 baseroute="https://demo.toucantoco.com")
    data_source = HttpAPIDataSource(name="myHttpDataSource", domain="my_domain", url="/")

    with pytest.raises(ValueError):
        connector.get_df(data_source)


def test_exceptions_wrong_filter(connector, data_source):
    data_source.filter = "bla"

    with pytest.raises(ValueError):
        connector.get_df(data_source)


def test_e2e():
    con_params = {
        'name': 'open_data_paris',
        'baseroute': 'https://opendata.paris.fr/api/'
    }
    ds_params = {
        'domain': 'books',
        'name': 'open_data_paris',
        'url': "records/1.0/search/",
        'params': {
            'dataset': 'les-1000-titres-les-plus-reserves-dans-les-bibliotheques-de-pret',
            'facet': 'auteur',
            'sort': 'rang',
            'rows': 1000
        },
        'filter': ".records[].fields"
    }

    con = HttpAPIConnector(**con_params)
    ds = HttpAPIDataSource(**ds_params)
    df = con.get_df(ds)
    assert df.shape == (1000, 5)


def test_get_df_with_json(connector, data_source, mocker):
    data_source.json = {'a': 1}
    mock = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mock.return_value.json.return_value = []
    connector.get_df(data_source)
    _, ke = mock.call_args

    assert ke['json'] == data_source.json


def test_get_df_with_template(data_source, mocker):
    co = HttpAPIConnector(**{'name': 'test', 'type': 'HttpAPI',
                             'baseroute': '', 'template': {'headers': {'Authorization': 'XX'}}})
    mock = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mock.return_value.json.return_value = []
    co.get_df(data_source)
    _, ke = mock.call_args

    assert 'Authorization' in ke['headers']
    assert ke['headers']['Authorization'] == co.template.headers['Authorization']


def test_get_df_with_template_overide(data_source, mocker):
    co = HttpAPIConnector(**{'name': 'test', 'type': 'HttpAPI', 'baseroute': '',
                             'template': {'headers': {'Authorization': 'XX', 'B': 1}}})
    data_source.headers = {'Authorization': 'YY'}
    data_source.json = {'A': 1}
    mock = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mock.return_value.json.return_value = []
    co.get_df(data_source)
    _, ke = mock.call_args

    assert 'Authorization' in ke['headers']
    assert ke['headers']['Authorization'] == data_source.headers['Authorization']
    assert 'B' in ke['headers'] and ke['headers']['B']
    assert 'A' in ke['json'] and ke['json']['A']


@pytest.mark.skip(reason="This uses an real api")
def test_get_df_oauth2_backend():

    data_provider = {
        'name': 'test',
        'type': 'HttpAPI',
        'baseroute': 'https://gateway.eu1.mindsphere.io/api/im/v3',
        'auth': {
            'type': 'oauth2_backend',
            'args': ['https://mscenter.piam.eu1.mindsphere.io/oauth/token',
                     '<client_id>',
                     '<client_secret>']
        }
    }

    users = {
        'domain': 'test',
        'name': 'test',
        'url': '/Users',
        'filter': '.resources'}

    co = HttpAPIConnector(**data_provider)
    df = co.get_df(HttpAPIDataSource(**users))
    assert "userName" in df
