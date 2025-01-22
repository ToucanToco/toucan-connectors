from unittest.mock import Mock, patch

import pytest
from pytest_mock import MockerFixture
from redshift_connector.error import InterfaceError, OperationalError, ProgrammingError

from toucan_connectors.pagination import (
    KnownSizeDatasetPaginationInfo,
    OffsetLimitInfo,
    PaginationInfo,
)
from toucan_connectors.redshift.redshift_database_connector import (
    ORDERED_KEYS,
    AuthenticationMethod,
    AuthenticationMethodError,
    RedshiftConnector,
    RedshiftDataSource,
)
from toucan_connectors.toucan_connector import DataSlice

CLUSTER_IDENTIFIER: str = "toucan_test"
DATABASE_NAME: str = "toucan"


@pytest.fixture
def redshift_connector():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
        name="test",
        host="http://localhost",
        port=0,
        cluster_identifier=CLUSTER_IDENTIFIER,
        user="user",
        password="sample",
        default_database="dev",
        connect_timeout=10,
    )


@pytest.fixture
def redshift_connector_aws_creds():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
        name="test",
        host="localhost",
        port=0,
        db_user="db_user_test",
        cluster_identifier=CLUSTER_IDENTIFIER,
        access_key_id="access_key",
        secret_access_key="secret_access_key",
        session_token="token",
        default_database="dev",
        region="eu-west-1",
    )


@pytest.fixture
def redshift_connector_aws_profile():
    return RedshiftConnector(
        authentication_method=AuthenticationMethod.AWS_PROFILE.value,
        name="test",
        host="localhost",
        port=0,
        db_user="db_user_test",
        cluster_identifier=CLUSTER_IDENTIFIER,
        profile="sample",
        default_database="dev",
        region="eu-west-1",
    )


@pytest.fixture
def redshift_datasource():
    return RedshiftDataSource(
        domain="test",
        name="redshift",
        database=DATABASE_NAME,
        query="SELECT * FROM public.sales;",
    )


def test_model_json_schema():
    schema = RedshiftConnector.model_json_schema()
    for key, expected_key in zip(schema["properties"].keys(), ORDERED_KEYS, strict=False):
        assert key == expected_key


def test_redshiftdatasource_init_(redshift_datasource):
    ds = RedshiftDataSource(domain="test", name="redshift", database="test")
    assert ds.language == "sql"
    assert hasattr(ds, "query_object")


def test_redshiftdatasource_get_form(redshift_connector, redshift_datasource, mocker: MockerFixture):
    current_config = {"database": "dev"}
    mocker.patch.object(RedshiftConnector, "available_dbs", new=["one", "two"])
    result = redshift_datasource.get_form(redshift_connector, current_config)
    assert result["properties"]["parameters"]["title"] == "Parameters"
    assert result["properties"]["domain"]["title"] == "Domain"
    assert result["properties"]["validation"]["title"] == "Validation"
    assert result["required"] == ["domain", "name"]


def test_redshiftconnector_get_connection_params_missing_authentication_mode():
    with pytest.raises(ValueError) as exc_info_user:
        RedshiftConnector(
            name="test",
            host="localhost",
            cluster_identifier="sample",
            port=0,
        )
    assert AuthenticationMethodError.UNKNOWN.value in str(exc_info_user.value)


def test_redshiftconnector_get_connection_params_db_cred_mode_missing_params():
    with pytest.raises(ValueError) as exc_info_user:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
            name="test",
            cluster_identifier="sample",
            host="localhost",
            port=0,
            password="pass",
        )
    assert AuthenticationMethodError.DB_CREDENTIALS.value in str(exc_info_user.value)

    # TODO: Partial check due to missing context in some operations (Missing: password)
    # with pytest.raises(ValueError) as exc_info_pwd:
    #     RedshiftConnector(
    #         authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
    #         name='test',
    #         cluster_identifier='sample',
    #         host='localhost',
    #         port=0,
    #         user='user',
    #     )
    # assert AuthenticationMethodError.DB_CREDENTIALS.value in str(exc_info_pwd.value)


def test_redshiftconnector_get_connection_params_db_cred_mode(redshift_connector):
    result = redshift_connector._get_connection_params(database="test")
    assert result == {
        "host": "localhost",
        "database": "test",
        "cluster_identifier": "toucan_test",
        "port": 0,
        "timeout": 10,
        "user": "user",
        "password": "sample",
        "tcp_keepalive": True,
    }


def test_redshiftconnector_get_connection_params_aws_creds_mode_missing_params():
    # TODO: Partial check due to missing context in some operations (Missing: secret_access_key)
    # with pytest.raises(ValueError) as exc_info_secret:
    #     RedshiftConnector(
    #         authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
    #         name='test',
    #         cluster_identifier='sample',
    #         host='localhost',
    #         port=0,
    #         db_user='db_user_test',
    #         access_key_id='access_key',
    #         session_token='token',
    #         region='eu-west-1',
    #     )
    # assert AuthenticationMethodError.AWS_CREDENTIALS.value in str(exc_info_secret.value)
    with pytest.raises(ValueError) as exc_info_key:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
            name="test",
            cluster_identifier="sample",
            host="localhost",
            port=0,
            db_user="db_user_test",
            secret_access_key="secret_access_key",
            session_token="token",
            region="eu-west-1",
        )
    assert AuthenticationMethodError.AWS_CREDENTIALS in str(exc_info_key.value)
    with pytest.raises(ValueError) as exc_info_db_user:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_CREDENTIALS.value,
            name="test",
            cluster_identifier="sample",
            host="localhost",
            port=0,
            access_key_id="access_key",
            secret_access_key="secret_access_key",
            session_token="token",
            region="eu-west-1",
        )
    assert AuthenticationMethodError.AWS_CREDENTIALS.value in str(exc_info_db_user.value)


def test_redshiftconnector_get_connection_params_aws_creds_mode(redshift_connector_aws_creds):
    result = redshift_connector_aws_creds._get_connection_params(database="test")
    assert result == {
        "host": "localhost",
        "database": "test",
        "port": 0,
        "iam": True,
        "db_user": "db_user_test",
        "cluster_identifier": "toucan_test",
        "access_key_id": "access_key",
        "secret_access_key": "secret_access_key",
        "session_token": "token",
        "region": "eu-west-1",
        "tcp_keepalive": True,
    }


def test_redshiftconnector_get_connection_params_aws_profile_mode_missing_params():
    with pytest.raises(ValueError) as exc_info_profile:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_PROFILE.value,
            name="test",
            cluster_identifier="toucan_test",
            host="localhost",
            port=0,
            db_user="db_user_test",
            region="eu-west-1",
        )
    assert AuthenticationMethodError.AWS_PROFILE.value in str(exc_info_profile.value)

    with pytest.raises(ValueError) as exc_info_db_user:
        RedshiftConnector(
            authentication_method=AuthenticationMethod.AWS_PROFILE.value,
            name="test",
            cluster_identifier="sample",
            host="localhost",
            port=0,
            profile="profile",
            region="eu-west-1",
        )
    assert AuthenticationMethodError.AWS_PROFILE.value in str(exc_info_db_user.value)


def test_redshiftconnector_get_connection_params_aws_profile_mode(redshift_connector_aws_profile):
    result = redshift_connector_aws_profile._get_connection_params(database="test")
    assert result == {
        "host": "localhost",
        "database": "test",
        "port": 0,
        "iam": True,
        "db_user": "db_user_test",
        "cluster_identifier": "toucan_test",
        "region": "eu-west-1",
        "profile": "sample",
        "tcp_keepalive": True,
    }


@pytest.mark.parametrize("opt", (True, False))
def test_redshiftconnector_get_connection_tcp_keepalive(redshift_connector, opt: bool):
    redshift_connector.enable_tcp_keepalive = opt
    result = redshift_connector._get_connection_params(database="test")
    assert result == {
        "host": "localhost",
        "database": "test",
        "cluster_identifier": "toucan_test",
        "port": 0,
        "timeout": 10,
        "user": "user",
        "password": "sample",
        "tcp_keepalive": opt,
    }


@patch.object(RedshiftConnector, "_get_connection")
def test_redshiftconnector_retrieve_tables(mock_connection, redshift_connector, redshift_datasource):
    mock_connection().cursor().__enter__().fetchall.return_value = (
        ["table1"],
        ["table2"],
        ["table3"],
    )
    result = redshift_connector._retrieve_tables(database=redshift_datasource.database)
    assert result == ["table1", "table2", "table3"]


@patch.object(RedshiftConnector, "_get_connection")
@patch("toucan_connectors.redshift.redshift_database_connector.SqlQueryHelper")
def test_redshiftconnector_retrieve_data(
    mock_SqlQueryHelper,  # noqa: N803
    mock_get_connection,
    redshift_connector,
    redshift_datasource,
):
    mock_response = Mock()
    mock_SqlQueryHelper.count_query_needed.return_value = True
    mock_SqlQueryHelper.prepare_limit_query.return_value = Mock(), Mock()
    mock_SqlQueryHelper.prepare_count_query.return_value = Mock(), Mock()
    mock_get_connection().cursor().__enter__().fetch_dataframe.return_value = mock_response
    result = redshift_connector._retrieve_data(datasource=redshift_datasource, get_row_count=True)
    assert result == mock_response


@patch.object(RedshiftConnector, "_get_connection")
@patch("toucan_connectors.redshift.redshift_database_connector.SqlQueryHelper")
def test_redshiftconnector_retrieve_data_empty_result(
    mock_SqlQueryHelper,  # noqa: N803
    mock_get_connection,
    redshift_connector,
    redshift_datasource,
):
    mock_SqlQueryHelper.count_query_needed.return_value = True
    mock_SqlQueryHelper.prepare_limit_query.return_value = Mock(), Mock()
    mock_SqlQueryHelper.prepare_count_query.return_value = Mock(), Mock()
    mock_get_connection().cursor().__enter__().fetch_dataframe.return_value = None
    result = redshift_connector._retrieve_data(datasource=redshift_datasource, get_row_count=True)
    assert result.empty is True


@patch.object(RedshiftConnector, "_get_connection")
@patch("toucan_connectors.redshift.redshift_database_connector.SqlQueryHelper")
def test_redshiftconnector_retrieve_data_without_count(
    mock_SqlQueryHelper,  # noqa: N803
    mock_get_connection,
    redshift_connector,
    redshift_datasource,
):
    mock_response = Mock()
    mock_SqlQueryHelper.prepare_limit_query.return_value = Mock(), Mock()
    mock_get_connection().cursor().__enter__().fetch_dataframe.return_value = mock_response
    result = redshift_connector._retrieve_data(datasource=redshift_datasource, limit=10)
    assert result == mock_response


@patch.object(RedshiftConnector, "_retrieve_data")
def test_redshiftconnector_get_slice(mock_retreive_data, redshift_datasource, redshift_connector):
    mock_df = Mock()
    mock_df.__len__ = lambda x: 1
    type(mock_df).total_rows = [10]

    mock_retreive_data.return_value = mock_df
    result = redshift_connector.get_slice(
        data_source=redshift_datasource, permissions=None, offset=0, limit=1, get_row_count=True
    )
    assert result == DataSlice(
        df=mock_df,
        pagination_info=PaginationInfo(
            parameters=OffsetLimitInfo(offset=0, limit=1),
            pagination_info=KnownSizeDatasetPaginationInfo(total_rows=10, is_last_page=False),
            next_page=OffsetLimitInfo(offset=1, limit=1),
        ),
    )


@patch.object(RedshiftConnector, "_retrieve_data")
def test_redshiftconnector_get_slice_without_count(mock_retreive_data, redshift_datasource, redshift_connector):
    mock_df = Mock()
    mock_df.__len__ = lambda x: 10

    mock_retreive_data.return_value = mock_df
    result = redshift_connector.get_slice(data_source=redshift_datasource)
    assert result == DataSlice(
        df=mock_df,
        pagination_info=PaginationInfo(
            parameters=OffsetLimitInfo(offset=0, limit=None),
            pagination_info=KnownSizeDatasetPaginationInfo(total_rows=10, is_last_page=True),
        ),
    )


@patch.object(RedshiftConnector, "_retrieve_data")
def test_redshiftconnector_get_slice_df_is_none(mock_retreive_data, redshift_datasource, redshift_connector):
    mock_retreive_data.return_value = None
    result = redshift_connector.get_slice(data_source=redshift_datasource)
    assert result == DataSlice(
        df=None,
        pagination_info=PaginationInfo(
            parameters=OffsetLimitInfo(offset=0, limit=None),
            pagination_info=KnownSizeDatasetPaginationInfo(total_rows=0, is_last_page=True),
        ),
    )


def test_redshiftconnector__get_details(redshift_connector):
    result = redshift_connector._get_details(index=0, status=True)
    assert result == [
        ("Hostname resolved", True),
        ("Port opened", False),
        ("Authenticated", False),
        ("Default Database connection", False),
    ]


@patch.object(RedshiftConnector, "check_hostname")
@patch.object(RedshiftConnector, "check_port")
@patch("redshift_connector.connect")
def test_redshiftconnector_get_status_true(
    mock_check_hostname, mock_check_port, mock_redshift_connector, redshift_connector
):
    mock_check_hostname.return_value = "hostname_test"
    mock_check_port.return_value = "port_test"
    mock_redshift_connector.return_value = True
    result = redshift_connector.get_status()
    assert result.status is True
    assert result.error is None


@patch.object(RedshiftConnector, "check_hostname")
def test_redshiftconnector_get_status_with_error_host(mock_hostname, redshift_connector):
    mock_hostname.side_effect = InterfaceError("error mock")
    result = redshift_connector.get_status()
    assert isinstance(result.error, str)
    assert result.status is False
    assert str(result.error) == "error mock"


@patch.object(RedshiftConnector, "check_port")
def test_redshiftconnector_get_status_with_error_port(mock_port, redshift_connector):
    mock_port.side_effect = InterfaceError("error mock")
    result = redshift_connector.get_status()
    assert isinstance(result.error, str)
    assert result.status is False
    assert str(result.error) == "error mock"


@patch.object(RedshiftConnector, "_get_connection")
def test_redshiftconnector_describe(mock_connection, redshift_connector, redshift_datasource):
    mock_description = Mock()
    type(mock_description).description = [
        (b"salesid", 23, None, None, None),
        (b"listid", 23, None, None, None),
        (b"pricepaid", 1700, None, None, None),
    ]
    mock_connection().cursor().__enter__.return_value = mock_description
    result = redshift_connector.describe(data_source=redshift_datasource)
    expected = {"salesid": "INTEGER", "listid": "INTEGER", "pricepaid": "DECIMAL"}
    assert result == expected


def test_get_model(mocker, redshift_connector):
    db_names_mock = mocker.patch.object(RedshiftConnector, "_list_db_names", return_value=["dev"])
    table_info_mock = mocker.patch.object(RedshiftConnector, "_db_table_info_rows")
    table_info_mock.return_value = [
        ("pg_internal", "redshift_auto_health_check_436837", "a", "integer"),
        ("public", "table_1", "label", "character varying"),
        ("public", "table_1", "doum", "character varying"),
        ("public", "table_1", "value1", "bigint"),
        ("public", "table_2", "label", "character varying"),
        ("public", "table_2", "doum", "character varying"),
        ("public", "table_2", "value1", "bigint"),
        ("public", "table_2", "value2", "bigint"),
        ("public", "table_3", "label", "character varying"),
        ("public", "table_3", "group", "character varying"),
    ]
    assert redshift_connector.get_model() == [
        {
            "database": "dev",
            "schema": "pg_internal",
            "name": "redshift_auto_health_check_436837",
            "type": "table",
            "columns": [{"name": "a", "type": "integer"}],
        },
        {
            "database": "dev",
            "schema": "public",
            "name": "table_1",
            "type": "table",
            "columns": [
                {"name": "label", "type": "character varying"},
                {"name": "doum", "type": "character varying"},
                {"name": "value1", "type": "bigint"},
            ],
        },
        {
            "database": "dev",
            "schema": "public",
            "name": "table_2",
            "type": "table",
            "columns": [
                {"name": "label", "type": "character varying"},
                {"name": "doum", "type": "character varying"},
                {"name": "value1", "type": "bigint"},
                {"name": "value2", "type": "bigint"},
            ],
        },
        {
            "database": "dev",
            "schema": "public",
            "name": "table_3",
            "type": "table",
            "columns": [
                {"name": "label", "type": "character varying"},
                {"name": "group", "type": "character varying"},
            ],
        },
    ]
    db_names_mock.assert_called_once()
    table_info_mock.assert_called_once_with("dev")
    db_names_mock.reset_mock()
    table_info_mock.reset_mock()

    redshift_connector.get_model("other-db")
    db_names_mock.assert_not_called()
    table_info_mock.assert_called_once_with("other-db")

    for error in [OperationalError, ProgrammingError]:
        mocker.patch.object(RedshiftConnector, "_db_tables_info", side_effect=error("oups"))

        assert redshift_connector.get_model() == []


def test_get_model_with_info(mocker, redshift_connector):
    db_names_mock = mocker.patch.object(RedshiftConnector, "_list_db_names", return_value=["dev"])
    list_table_info_mock = mocker.patch.object(
        RedshiftConnector,
        "_list_tables_info",
        return_value=[
            {
                "database": "dev",
                "schema": "public",
                "type": "table",
                "name": "cool",
                "columns": [{"name": "foo", "type": "bar"}, {"name": "roo", "type": "far"}],
            }
        ],
    )

    assert redshift_connector.get_model_with_info() == (
        [
            {
                "columns": [{"name": "foo", "type": "bar"}, {"name": "roo", "type": "far"}],
                "database": "dev",
                "name": "cool",
                "schema": "public",
                "type": "table",
            }
        ],
        {},
    )
    db_names_mock.assert_called_once()
    list_table_info_mock.assert_called_once_with("dev")
    db_names_mock.reset_mock()
    list_table_info_mock.reset_mock()

    redshift_connector.get_model_with_info("other-db")
    db_names_mock.assert_not_called()
    list_table_info_mock.assert_called_once_with("other-db")

    # on error
    for error in [OperationalError, ProgrammingError]:
        mocker.patch.object(RedshiftConnector, "_list_tables_info", side_effect=error("oups"))

        assert redshift_connector.get_model_with_info() == (
            [],
            {"info": {"Could not reach databases": ["dev"]}},
        )
