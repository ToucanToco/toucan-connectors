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
    data_source.auth = auth
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
    data_source.auth = auth
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
