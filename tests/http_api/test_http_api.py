from xml.etree.ElementTree import ParseError

import pytest
import requests
import responses
from pytest_mock import MockFixture

from toucan_connectors.common import transform_with_jq
from toucan_connectors.http_api.http_api_connector import (
    Auth,
    HttpAPIConnector,
    HttpAPIConnectorError,
    HttpAPIDataSource,
)
from toucan_connectors.http_api.pagination_configs import (
    CursorBasedPaginationConfig,
    HyperMediaPaginationConfig,
    OffsetLimitPaginationConfig,
    PageBasedPaginationConfig,
)
from toucan_connectors.json_wrapper import JsonWrapper


@pytest.fixture
def xml_connector():
    data_provider = {
        "name": "APIreturningXML",
        "type": "HttpAPI",
        "baseroute": "http://example.com/api/v1",
        "responsetype": "xml",
    }
    return HttpAPIConnector(**data_provider)


@pytest.fixture()
def xml_datasource():
    data_source = {
        "domain": "testxml",
        "name": "XMLAPI",
        "url": "foo/xml",
        "method": "GET",
        "xpath": "output",
        "filter": ".output.users.user",
    }
    return HttpAPIDataSource(**data_source)


@pytest.fixture(scope="function")
def connector():
    return HttpAPIConnector(name="myHttpConnector", type="HttpAPI", baseroute="https://jsonplaceholder.typicode.com")


@pytest.fixture(scope="function")
def data_source():
    return HttpAPIDataSource(name="myHttpDataSource", domain="my_domain", url="/comments")


@pytest.fixture(scope="function")
def auth():
    return Auth(type="basic", args=["username", "password"])


@pytest.fixture(scope="function")
def offset_pagination() -> OffsetLimitPaginationConfig:
    return OffsetLimitPaginationConfig(
        kind="OffsetLimitPaginationConfig", offset_name="super_offset", limit_name="super_limit", limit=5
    )


@pytest.fixture(scope="function")
def page_pagination() -> PageBasedPaginationConfig:
    return PageBasedPaginationConfig(
        kind="PageBasedPaginationConfig", page_name="my_page", per_page_name="my_per_page", per_page=2, page=1
    )


@pytest.fixture(scope="function")
def cursor_pagination() -> CursorBasedPaginationConfig:
    return CursorBasedPaginationConfig(
        kind="CursorBasedPaginationConfig", cursor_name="my_cursor", cursor_filter=".metadata.next_cursor"
    )


@pytest.fixture(scope="function")
def hyper_media_pagination() -> HyperMediaPaginationConfig:
    return HyperMediaPaginationConfig(kind="HyperMediaPaginationConfig", next_link_filter=".metadata.next_link")


def test_transform_with_jq():
    assert transform_with_jq(data=[1, 2, 3], jq_filter=".[]+1") == [2, 3, 4]
    assert transform_with_jq([[1, 2, 3]], ".[]") == [1, 2, 3]
    assert transform_with_jq([{"col1": [1, 2], "col2": [3, 4]}], ".") == [{"col1": [1, 2], "col2": [3, 4]}]


@responses.activate
def test_get_df(connector: HttpAPIConnector, data_source: HttpAPIDataSource) -> None:
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments",
        json=[
            {"comment": "Hello there", "id": 1},
            {"comment": "Hello there", "id": 2},
            {"comment": "Hello there", "id": 3},
        ],
    )
    df = connector.get_df(data_source)
    assert df.shape == (3, 2)


@responses.activate
def test_get_df_with_auth(connector, data_source, auth):
    responses.add(responses.GET, "https://jsonplaceholder.typicode.com/comments", json=[{"a": 1}])

    connector.auth = auth
    connector.get_df(data_source)

    assert "Authorization" in responses.calls[0].request.headers
    assert responses.calls[0].request.headers["Authorization"].startswith("Basic")


@responses.activate
def test_get_df_with_offset_pagination(
    connector: HttpAPIConnector, data_source: HttpAPIDataSource, offset_pagination: OffsetLimitPaginationConfig
) -> None:
    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?super_offset=0&super_limit=5",
        json=[{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}],
    )

    # second page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?super_offset=5&super_limit=5",
        json=[
            {"a": 6},
            {"a": 7},
            {"a": 8},
            {"b": 9},
            {"b": 10},
        ],
    )

    # last page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?super_offset=10&super_limit=5",
        json=[
            {"b": 11},
            {"b": 12},
        ],
    )

    connector.http_pagination_config = offset_pagination
    df = connector.get_df(data_source)
    assert df.shape == (12, 2)
    assert len(responses.calls) == 3


@responses.activate
def test_get_df_with_page_pagination(
    connector: HttpAPIConnector, data_source: HttpAPIDataSource, page_pagination: PageBasedPaginationConfig
) -> None:
    page_pagination.max_page_filter = ".metadata.number_of_pages"

    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?my_page=1&my_per_page=2",
        json={
            "content": [
                {"a": 1},
                {"a": 2},
            ],
            "metadata": {"number_of_pages": 2},
        },
    )

    # next page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?my_page=2&my_per_page=2",
        json={
            "content": [
                {"a": 3},
                {"a": 4},
            ],
            "metadata": {"number_of_pages": 2},
        },
    )

    data_source.filter = ".content"
    connector.http_pagination_config = page_pagination
    df = connector.get_df(data_source)
    assert df.shape == (4, 1)
    assert len(responses.calls) == 2


@responses.activate
def test_get_df_with_page_pagination_which_can_raise(
    connector: HttpAPIConnector, data_source: HttpAPIDataSource, page_pagination: PageBasedPaginationConfig
) -> None:
    page_pagination.can_raise_not_found = True

    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?my_page=1&my_per_page=2",
        json={
            "content": [
                {"a": 1},
                {"a": 2},
            ],
        },
    )

    # next page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?my_page=2&my_per_page=2",
        json={
            "content": [
                {"a": 3},
                {"a": 4},
            ],
        },
    )

    # not found
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?my_page=3&my_per_page=2",
        json={"error": "not found"},
        status=404,
    )

    data_source.filter = ".content"
    connector.http_pagination_config = page_pagination
    df = connector.get_df(data_source)
    assert df.shape == (4, 1)
    assert len(responses.calls) == 3


@responses.activate
def test_get_df_with_cursor_pagination(
    connector: HttpAPIConnector, data_source: HttpAPIDataSource, cursor_pagination: CursorBasedPaginationConfig
) -> None:
    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments",
        json={
            "content": [
                {"a": 1},
                {"a": 2},
            ],
            "metadata": {"next_cursor": "super_cursor_22222", "number_of_results": 4},
        },
    )

    # next page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?my_cursor=super_cursor_22222",
        json={
            "content": [
                {"a": 3},
                {"a": 4},
            ],
            "metadata": {"number_of_results": 4},
        },
    )
    connector.http_pagination_config = cursor_pagination
    data_source.filter = ".content"
    df = connector.get_df(data_source)
    assert df.shape == (4, 1)
    assert len(responses.calls) == 2


@responses.activate
def test_get_df_with_hyper_media_pagination(
    connector: HttpAPIConnector, data_source: HttpAPIDataSource, hyper_media_pagination: HyperMediaPaginationConfig
) -> None:
    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?custom=yes",
        json={
            "content": [
                {"a": 1},
                {"a": 2},
            ],
            "metadata": {
                "next_link": "https://jsonplaceholder.typicode.com/comments/next_link?token=12341243&custom=yes",
                "number_of_results": 4,
            },
        },
    )

    # next page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments/next_link?token=12341243&custom=yes",
        json={
            "content": [
                {"a": 3},
                {"a": 4},
            ],
            "metadata": {"number_of_results": 4},
        },
    )
    connector.http_pagination_config = hyper_media_pagination
    data_source.filter = ".content"
    data_source.params = {"custom": "yes"}
    df = connector.get_df(data_source)
    assert df.shape == (4, 1)
    assert len(responses.calls) == 2


@responses.activate
def test_hyper_media_pagination_raise_if_bad_next_link(
    connector: HttpAPIConnector, data_source: HttpAPIDataSource, hyper_media_pagination: HyperMediaPaginationConfig
) -> None:
    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments?custom=yes",
        json={
            "content": [
                {"a": 1},
                {"a": 2},
            ],
            "metadata": {
                "next_link": {"real_link": "my_link"},
                "number_of_results": 4,
            },
        },
    )

    connector.http_pagination_config = hyper_media_pagination
    data_source.filter = ".content"
    data_source.params = {"custom": "yes"}
    with pytest.raises(ValueError) as exc:
        connector.get_df(data_source)
    assert str(exc.value) == (
        "Invalid next link value. Link can't be a complex value," " got: {'real_link': 'my_link'}"
    )


@responses.activate
def test_ignore_if_cant_parse_next_pagination_info(
    connector: HttpAPIConnector, data_source: HttpAPIDataSource, hyper_media_pagination: HyperMediaPaginationConfig
) -> None:
    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments",
        json=[
            {"a": 1},
            {"a": 2},
        ],
    )

    connector.http_pagination_config = hyper_media_pagination  # needs a 'metadata' field to retrieve the next link
    # Ok even if 'metadata' is missing in the API response
    df = connector.get_df(data_source)
    assert df.shape == (2, 1)
    assert len(responses.calls) == 1


@responses.activate
def test_raises_http_error_on_too_many_requests(connector: HttpAPIConnector, data_source: HttpAPIDataSource) -> None:
    # first page
    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments",
        json=[
            {"a": 1},
            {"a": 2},
        ],
        status=429,
    )
    with pytest.raises(HttpAPIConnectorError) as exc:
        connector.get_df(data_source)
    assert str(exc.value) == (
        "Failed to retrieve data: the connector tried to perform too many requests."
        " Please check your API call limitations."
    )


@responses.activate
def test_get_df_with_parameters(connector, data_source):
    data_source.parameters = {"first_name": "raphael"}
    data_source.headers = {"name": "%(first_name)s"}

    responses.add(responses.GET, "https://jsonplaceholder.typicode.com/comments", json=[{"a": 1}])

    connector.get_df(data_source)

    assert "name" in responses.calls[0].request.headers
    assert responses.calls[0].request.headers["name"] == "raphael"


@responses.activate
def test_get_df_with_parameters_and_auth(connector, data_source, auth, mocker):
    connector.auth = auth
    data_source.parameters = {"first_name": "raphael"}
    data_source.headers = {"name": "%(first_name)s"}

    responses.add(responses.GET, "https://jsonplaceholder.typicode.com/comments", json=[{"a": 1}])

    connector.get_df(data_source)

    assert "name" in responses.calls[0].request.headers
    assert responses.calls[0].request.headers["name"] == "raphael"


def test_exceptions_not_json():
    connector = HttpAPIConnector(name="myHttpConnector", type="HttpAPI", baseroute="https://demo.toucantoco.com")
    data_source = HttpAPIDataSource(name="myHttpDataSource", domain="my_domain", url="/")

    with pytest.raises(ValueError):
        connector.get_df(data_source)


def test_exceptions_wrong_filter(connector, data_source):
    data_source.filter = "bla"

    with pytest.raises(ValueError):
        connector.get_df(data_source)


def test_e2e():
    con_params = {"name": "open_data_paris", "baseroute": "https://opendata.paris.fr/api/"}
    ds_params = {
        "domain": "books",
        "name": "open_data_paris",
        "url": "records/1.0/search/",
        "params": {
            "dataset": "les-1000-titres-les-plus-reserves-dans-les-bibliotheques-de-pret",
            "facet": "auteur",
            "sort": "rang",
            "rows": 1000,
        },
        "filter": ".records[].fields",
    }

    con = HttpAPIConnector(**con_params)
    ds = HttpAPIDataSource(**ds_params)
    df = con.get_df(ds)
    assert df.shape == (1000, 5)


@responses.activate
def test_get_df_with_json(connector, data_source, mocker):
    data_source = HttpAPIDataSource(name="myHttpDataSource", domain="my_domain", url="/comments", json={"a": 1})

    responses.add(responses.GET, "https://jsonplaceholder.typicode.com/comments", json=[{"a": 2}])

    connector.get_df(data_source)

    assert responses.calls[0].request.body == b'{"a": 1}'


@responses.activate
def test_get_df_with_json_flatten_column(connector, data_source, mocker):
    data_source = HttpAPIDataSource(
        name="myHttpDataSource",
        domain="my_domain",
        url="/comments",
        flatten_column="products",
    )

    responses.add(
        responses.GET,
        "https://jsonplaceholder.typicode.com/comments",
        json={"brand": "brewdog", "products": [{"name": "punk"}, {"name": "5pm"}]},
    )

    result = connector.get_df(data_source)
    assert list(result["products.name"]) == ["punk", "5pm"]


@responses.activate
def test_get_df_with_template(data_source, mocker):
    co = HttpAPIConnector(
        **{
            "name": "test",
            "type": "HttpAPI",
            "baseroute": "http://example.com",
            "template": {"headers": {"Authorization": "XX"}},
        }
    )

    responses.add(responses.GET, "http://example.com/comments", json=[{"a": 2}])

    co.get_df(data_source)

    h = responses.calls[0].request.headers
    assert "Authorization" in h
    assert h["Authorization"] == co.template.headers["Authorization"]


@responses.activate
def test_get_df_with_template_overide(data_source, mocker):
    co = HttpAPIConnector(
        **{
            "name": "test",
            "type": "HttpAPI",
            "baseroute": "http://example.com",
            "template": {"headers": {"Authorization": "XX", "B": "1"}},
        }
    )

    data_source = HttpAPIDataSource(
        name="myHttpDataSource",
        domain="my_domain",
        url="/comments",
        json={"A": 1},
        headers={"Authorization": "YY"},
    )

    responses.add(responses.GET, "http://example.com/comments", json=[{"a": 2}])

    co.get_df(data_source)

    h = responses.calls[0].request.headers
    j = JsonWrapper.loads(responses.calls[0].request.body)
    assert "Authorization" in h
    assert h["Authorization"] == data_source.headers["Authorization"]
    assert "B" in h and h["B"]
    assert "A" in j and j["A"]


@pytest.mark.skip(reason="This uses an real api")
def test_get_df_oauth2_backend():
    data_provider = {
        "name": "test",
        "type": "HttpAPI",
        "baseroute": "https://gateway.eu1.mindsphere.io/api/im/v3",
        "auth": {
            "type": "oauth2_backend",
            "args": [
                "https://mscenter.piam.eu1.mindsphere.io/oauth/token",
                "<client_id>",
                "<client_secret>",
            ],
        },
    }

    users = {"domain": "test", "name": "test", "url": "/Users", "filter": ".resources"}

    co = HttpAPIConnector(**data_provider)
    df = co.get_df(HttpAPIDataSource(**users))
    assert "userName" in df


@responses.activate
def test_get_df_oauth2_backend_mocked():
    data_provider = {
        "name": "test",
        "type": "HttpAPI",
        "baseroute": "https://gateway.eu1.mindsphere.io/api/im/v3",
        "auth": {
            "type": "oauth2_backend",
            "args": [
                "https://mscenter.piam.eu1.mindsphere.io/oauth/token",
                "<client_id>",
                "<client_secret>",
            ],
        },
    }

    users = {"domain": "test", "name": "test", "url": "/Users"}

    responses.add(
        responses.POST,
        "https://mscenter.piam.eu1.mindsphere.io/oauth/token",
        json={"access_token": "A"},
    )
    responses.add(responses.GET, "https://gateway.eu1.mindsphere.io/api/im/v3/Users", json=[{"A": 1}])

    co = HttpAPIConnector(**data_provider)
    co.get_df(HttpAPIDataSource(**users))

    assert len(responses.calls) == 2


def test_with_proxies(mocker):
    req = mocker.patch("toucan_connectors.http_api.http_api_connector.Session.request")
    f = "toucan_connectors.http_api.http_api_connector.transform_with_jq"
    mocker.patch(f).return_value = [{"a": 1}]

    data_provider = {"name": "test", "type": "HttpApi", "baseroute": "https://example.com"}

    data_source = {
        "proxies": {"https": "https://eu1.proxysite.com"},
        "name": "test",
        "domain": "test_domain",
        "url": "/endpoint",
    }

    HttpAPIConnector(**data_provider).get_df(HttpAPIDataSource(**data_source))
    args, kwargs = req.call_args
    assert kwargs["proxies"] == {"https": "https://eu1.proxysite.com"}


def test_with_cert(mocker):
    req = mocker.patch("toucan_connectors.http_api.http_api_connector.Session.request")
    f = "toucan_connectors.http_api.http_api_connector.transform_with_jq"
    mocker.patch(f).return_value = [{"a": 1}]

    data_provider = {
        "name": "test",
        "type": "HttpApi",
        "baseroute": "https://example.com",
        "cert": ["tests/http_api/test_http_api.py", "tests/http_api/test_http_api.py"],
    }

    data_source = {"name": "test", "domain": "test_domain", "url": "/endpoint"}

    HttpAPIConnector(**data_provider).get_df(HttpAPIDataSource(**data_source))
    args, kwargs = req.call_args
    assert kwargs["cert"] == ["tests/http_api/test_http_api.py", "tests/http_api/test_http_api.py"]


@responses.activate
def test_no_top_level_domain():
    data_provider = {
        "name": "DataServiceApi",
        "type": "HttpAPI",
        # before we relaxed the type of baseroute using AnyHttpUrl
        # this "domain" would trigger a validation error
        "baseroute": "http://cd-arggh-v2:9088/api/aggregations/v1/",
    }
    c = HttpAPIConnector(**data_provider)

    # we want to check as well that we call the right urls
    data_source = {
        "domain": "appendable3_id_9001",
        "name": "DataServiceApi",
        "url": "forecast_90_days/site/%(SITE_VARIABLE)s/date/%(REQ_DATE)s",
        "parameters": {"SITE_VARIABLE": "blah", "REQ_DATE": "123456"},
        "method": "GET",
    }

    responses.add(
        responses.GET,
        "http://cd-arggh-v2:9088/api/aggregations/v1/forecast_90_days/site/blah/date/123456",
        json=[{"a": 1}],
    )
    c.get_df(HttpAPIDataSource(**data_source))

    assert len(responses.calls) == 1


@responses.activate
def test_parse_xml_error(xml_connector, xml_datasource):
    """
    Check that the http connector returns an error if the XML
    is invalid
    """
    responses.add(
        responses.GET,
        "http://example.com/api/v1/foo/xml",
        content_type="application/xml",
        body="""<?xml version='1.0' encoding='UTF-8'?><response success='true'>
        <output><usThis is a broken XML</output></response>""",
    )

    with pytest.raises(ParseError):
        xml_connector.get_df(xml_datasource)


@responses.activate
def test_parse_xml_response(xml_connector, xml_datasource):
    """
    Check that the http connector is able to parse an XML response
    """
    responses.add(
        responses.GET,
        "http://example.com/api/v1/foo/xml",
        content_type="application/xml",
        body="""<?xml version='1.0' encoding='UTF-8'?>
            <response success="true">
            <output>
                <users seqNo="55">
                    <user id="19" guid="B9ADBCB81AA2F9BAE040307F02092C2E" login="analytica@fakecompany.com" email="analytica@fakecompany.com"
                    name="Anna Analyzer" roleId="3" timeZone="US/Pacific"/>
                    <user id="123" guid="AAFF5218D55ABB9234660001BEC117A9" login="randomuser@fakecompany.com" email="randomuser@fakecompany.com"
                    name="J. Random User" roleId="2" timeZone="US/Pacific"/>
                </users>
            </output>
            </response>""",  # noqa: E501
    )
    df = xml_connector.get_df(xml_datasource)
    assert df["login"][0] == "analytica@fakecompany.com"
    assert df["timeZone"][0] == "US/Pacific"
    assert df["id"][1] == "123"


@responses.activate
def test_oauth2_oidc_authentication(mocker):
    data_provider = {
        "type": "HttpAPI",
        "name": "bidule-api",
        "baseroute": "https://api.bidule.com",
        "auth": {
            "type": "oauth2_oidc",
            "args": [],
            "kwargs": {
                "id_token": "id_token_test",
                "refresh_token": "refresh_tokenètest",
                "client_id": "provided_client_id",
                "client_secret": "provided_client_secret",
                "token_endpoint": "https://api.bidule.com/token",
            },
        },
    }
    data_source = {
        "domain": "test",
        "name": "bidule-api",
        "url": "/data",
        "parameters": {"some": "variable"},
        "method": "GET",
    }
    c = HttpAPIConnector(**data_provider)
    session = requests.Session()
    session.headers.update({"Authorization": "Bearer MyNiceToken"})
    mock_session = mocker.patch("toucan_connectors.auth.oauth2_oidc")
    responses.add(method=responses.GET, url="https://api.bidule.com/data", json={"ultimecia": "citadel"})
    c.get_df(HttpAPIDataSource(**data_source))
    mock_session.assert_called_once()


def test_model_json_schema():
    data_source_spec = {
        "data": "",
        "domain": "Clickhouse test",
        "filter": "",
        "flatten_column": "",
        "headers": {},
        "json": {},
        "live_data": False,
        "load": True,
        "method": "GET",
        "name": "Some clickhouse provider",
        "parameters": {"ids": [3986, 3958]},
        "params": {},
        "proxies": {},
        "type": "external_database",
        "url": "",
        "validation": {},
        "xpath": "",
    }
    ds = HttpAPIDataSource(**data_source_spec)
    assert list(ds.model_json_schema()["properties"].keys())[-6:] == [
        "proxies",
        "flatten_column",
        "data",
        "xpath",
        "filter",
        "validation",
    ]


def test_get_cache_key(connector, auth, data_source):
    data_source.headers = {"name": "%(first_name)s"}
    data_source.parameters = {"first_name": "raphael"}
    key = connector.get_cache_key(data_source)

    assert key == "96f415db-63dc-37e4-96df-cb346942c815"

    data_source.headers = {"name": "{{ first_name }}"}  # change the templating style
    key2 = connector.get_cache_key(data_source)
    assert key2 == key  # same result because the templates are rendered

    data_source.parameters["nickname"] = "raph"  # add a useless parameter
    key3 = connector.get_cache_key(data_source)
    assert key3 == key  # same result because the new parameter does not impact the result

    key4 = connector.get_cache_key(data_source, offset=10)
    assert key4 != key  # adding an offset changed the result

    another_connector = connector.copy(update={"auth": auth})

    assert connector.get_cache_key(data_source) != another_connector.get_cache_key(data_source)


def test_response_json_fails(connector: HttpAPIConnector, mocker: MockFixture, data_source: HttpAPIDataSource) -> None:
    mocked_request = mocker.MagicMock(name="mocked_request")
    mocked_response = mocker.MagicMock(name="mocked_response")
    mocked_request.request.return_value = mocked_response
    mocked_response.json.side_effect = ValueError
    mocked_request.return_value = mocked_response
    mocker.patch("toucan_connectors.http_api.http_api_connector.Session", return_value=mocked_request)
    mocked_loads = mocker.patch("toucan_connectors.http_api.http_api_connector.json.loads")
    mocker.patch("toucan_connectors.http_api.http_api_connector.transform_with_jq", return_value=[{"a": 1}])
    connector.get_df(data_source)
    mocked_loads.assert_called_once()


def test_instantiate_connector_basic_auth_by_kwargs() -> None:
    conn = HttpAPIConnector(
        **{
            "name": "aaaa",
            "baseroute": "https://jsonplaceholder.typicode.com/posts",
            "auth": {"kwargs": {"username": "a", "password": "b"}, "type": "basic"},
            "type": "HttpAPI",
        }
    )
    assert conn.auth == Auth(type="basic", kwargs={"username": "a", "password": "b"})
