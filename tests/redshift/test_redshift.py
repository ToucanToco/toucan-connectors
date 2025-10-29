import os
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture
from redshift_connector.error import InterfaceError, OperationalError, ProgrammingError
from tenacity import retry, stop_after_delay, wait_fixed

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
    mocker.patch.object(RedshiftConnector, "_list_db_names", return_value=["one", "two"])
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
    list_table_info_mock.assert_called_once_with(
        database_name="dev", schema_name=None, table_name=None, exclude_columns=False
    )
    db_names_mock.reset_mock()
    list_table_info_mock.reset_mock()

    redshift_connector.get_model_with_info("other-db")
    db_names_mock.assert_not_called()
    list_table_info_mock.assert_called_once_with(
        database_name="other-db", schema_name=None, table_name=None, exclude_columns=False
    )

    # on error
    for error in [OperationalError, ProgrammingError]:
        mocker.patch.object(RedshiftConnector, "_list_tables_info", side_effect=error("oups"))

        assert redshift_connector.get_model_with_info() == (
            [],
            {"info": {"Could not reach databases": ["dev"]}},
        )


# Retrying every 5 seconds for 60 seconds
@retry(stop=stop_after_delay(60), wait=wait_fixed(5))
def _ready_connector(connector: RedshiftConnector) -> RedshiftConnector:
    datasource = RedshiftDataSource(database="weaverbird_integration_tests", domain="d", name="n")
    datasource = datasource.model_copy(update={"query": 'SELECT 1 "1";'})
    df = connector._retrieve_data(datasource)
    assert_frame_equal(df, pd.DataFrame({"1": [1]}))

    return connector


_REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
_REDSHIFT_USER = os.environ["REDSHIFT_USER"]
_REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]


@pytest.fixture
def integration_redshift_connector() -> RedshiftConnector:
    connector = RedshiftConnector(
        authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
        name="test-connector",
        host=_REDSHIFT_HOST,
        port=5439,
        user=_REDSHIFT_USER,
        password=_REDSHIFT_PASSWORD,
        connect_timeout=10,
    )
    return _ready_connector(connector)


def test_get_model_integration_redshift_connector(integration_redshift_connector: RedshiftConnector) -> None:
    # Default
    model = integration_redshift_connector.get_model()
    assert model == [
        {
            "columns": [
                {"name": "id", "type": "int4"},
                {"name": "name", "type": "varchar"},
                {"name": "countrycode", "type": "bpchar"},
                {"name": "district", "type": "varchar"},
                {"name": "population", "type": "int4"},
            ],
            "database": "weaverbird_integration_tests",
            "name": "city",
            "schema": "other_schema",
            "type": "table",
        },
        {
            "columns": [
                {"name": "price_per_l", "type": "float8"},
                {"name": "alcohol_degree", "type": "float8"},
                {"name": "name", "type": "varchar"},
                {"name": "cost", "type": "float8"},
                {"name": "beer_kind", "type": "varchar"},
                {"name": "volume_ml", "type": "int8"},
                {"name": "brewing_date", "type": "timestamp"},
                {"name": "nullable_name", "type": "varchar"},
            ],
            "database": "weaverbird_integration_tests",
            "name": "beers_tiny",
            "schema": "public",
            "type": "table",
        },
    ]
    # Filter on schema
    model = integration_redshift_connector.get_model(schema_name="public")
    assert model == [
        {
            "columns": [
                {"name": "price_per_l", "type": "float8"},
                {"name": "alcohol_degree", "type": "float8"},
                {"name": "name", "type": "varchar"},
                {"name": "cost", "type": "float8"},
                {"name": "beer_kind", "type": "varchar"},
                {"name": "volume_ml", "type": "int8"},
                {"name": "brewing_date", "type": "timestamp"},
                {"name": "nullable_name", "type": "varchar"},
            ],
            "database": "weaverbird_integration_tests",
            "name": "beers_tiny",
            "schema": "public",
            "type": "table",
        },
    ]
    # Filter on DB + Table
    model = integration_redshift_connector.get_model(db_name="weaverbird_integration_tests", table_name="city")
    assert model == [
        {
            "columns": [
                {"name": "id", "type": "int4"},
                {"name": "name", "type": "varchar"},
                {"name": "countrycode", "type": "bpchar"},
                {"name": "district", "type": "varchar"},
                {"name": "population", "type": "int4"},
            ],
            "database": "weaverbird_integration_tests",
            "name": "city",
            "schema": "other_schema",
            "type": "table",
        },
    ]
