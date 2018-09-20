from toucan_connectors.http_api.http_api_connector import (
    HttpAPIConnector,
    HttpAPIDataSource,
    transform_with_jq,
    Auth,
    HTTPBasicAuth
)

HC = HttpAPIConnector(name="myHttpConnector", type="HttpAPI",
                      baseroute="https://jsonplaceholder.typicode.com")
HD = HttpAPIDataSource(name="myHttpDataSource", domain="my_domain", url="/comments")
AT = Auth(type="basic", args=["username", "password"])


def test_transform_with_jq():
    assert transform_with_jq(data=[1, 2, 3], jq_filter='.[]+1') == [2, 3, 4]
    assert transform_with_jq([[1, 2, 3]], '.[]') == [1, 2, 3]
    assert transform_with_jq(
        [{'col1': [1, 2], 'col2': [3, 4]}], '.') == [{'col1': [1, 2], 'col2': [3, 4]}]


def test_get_df():
    df = HC.get_df(HD)
    assert df.shape == (500, 5)


def test_get_df_with_auth(mocker):
    HD.auth = AT
    mocke = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mocke.return_value.json.return_value = []

    HC.get_df(HD)
    _, ke = mocke.call_args

    assert ke["auth"].username == 'username'
    assert isinstance(ke["auth"], HTTPBasicAuth)


def test_get_df_with_parameters(mocker):
    HD.auth = None
    HD.parameters = {"first_name": "raphael"}
    HD.headers = {"name": "%(first_name)s"}

    mocke = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mocke.return_value.json.return_value = []

    HC.get_df(HD)
    _, ke = mocke.call_args

    assert ke["headers"] == {"name": "raphael"}


def test_get_df_with_parameters_and_auth(mocker):
    HD.auth = AT
    HD.parameters = {"first_name": "raphael"}
    HD.headers = {"name": "%(first_name)s"}

    mocke = mocker.patch("toucan_connectors.http_api.http_api_connector.request")
    mocke.return_value.json.return_value = []

    HC.get_df(HD)
    _, ke = mocke.call_args

    assert ke["headers"] == {"name": "raphael"}
