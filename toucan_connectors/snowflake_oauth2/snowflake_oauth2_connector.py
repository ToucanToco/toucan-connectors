import concurrent
import logging
import uuid
from contextlib import suppress
from timeit import default_timer as timer
from typing import Any

from pydantic import Field, PrivateAttr, create_model

from toucan_connectors.connection_manager import ConnectionManager
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.snowflake.snowflake_connector import AuthenticationMethod
from toucan_connectors.snowflake_common import (
    SfDataSource,
    SnowflakeCommon,
    build_database_model_extraction_query,
)
from toucan_connectors.toucan_connector import (
    Category,
    DataSlice,
    DiscoverableConnector,
    PlainJsonSecretStr,
    TableInfo,
    ToucanConnector,
    strlist_to_enum,
)

_LOGGER = logging.getLogger(__name__)

try:
    import pandas as pd
    import snowflake
    from snowflake.connector import SnowflakeConnection

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    _LOGGER.warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False


connection_manager = None
if not connection_manager:
    connection_manager = ConnectionManager(
        name="snowflake_oauth2", timeout=5, wait=0.2, time_between_clean=3, time_keep_alive=600
    )


class SnowflakeoAuth2DataSource(SfDataSource):
    @classmethod
    def _get_databases(cls, connector: "SnowflakeoAuth2Connector"):
        return connector._get_databases()

    @classmethod
    def get_form(cls, connector: "SnowflakeoAuth2Connector", current_config):
        constraints = {}

        with suppress(Exception):
            databases = connector._get_databases()
            warehouses = connector._get_warehouses()
            # Restrict some fields to lists of existing counterparts
            constraints["database"] = strlist_to_enum("database", databases)
            constraints["warehouse"] = strlist_to_enum("warehouse", warehouses)

        res = create_model("FormSchema", **constraints, __base__=cls).schema()  # type: ignore[call-overload]
        res["properties"]["warehouse"]["default"] = connector.default_warehouse
        return res


class SnowflakeoAuth2Connector(ToucanConnector, data_source_model=SnowflakeoAuth2DataSource):
    client_id: str = Field(  # type: ignore[call-overload]
        "",
        title="Client ID",
        description="The client id of you Snowflake integration",
        **{"ui.required": True},
    )
    client_secret: PlainJsonSecretStr = Field(  # type: ignore[call-overload]
        "",
        title="Client Secret",
        description="The client secret of your Snowflake integration",
        **{"ui.required": True},
    )
    authorization_url: str = Field(None, **{"ui.hidden": True})  # type: ignore[call-overload]
    scope: str = Field(None, Title="Scope", description="The scope the integration", placeholder="refresh_token")  # type: ignore[call-overload]
    token_url: str = Field(None, **{"ui.hidden": True})  # type: ignore[call-overload]
    auth_flow_id: str = Field(None, **{"ui.hidden": True})  # type: ignore[call-overload]
    _auth_flow = "oauth2"
    _oauth_trigger = "connector"
    oauth2_version: str = Field("1", **{"ui.hidden": True})  # type: ignore[call-overload]
    redirect_uri: str | None = Field(None, **{"ui.hidden": True})  # type: ignore[call-overload]
    _oauth2_connector: OAuth2Connector = PrivateAttr()

    role: str = Field(  # type: ignore[call-overload]
        ...,
        title="Role",
        description="Role to use for queries",
        placeholder="PUBLIC",
    )
    account: str = Field(
        ...,
        title="Account",
        description="The full name of your Snowflake account. "
        "It might require the region and cloud platform where your account is located, "
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info" target="_blank">here</a>.',  # noqa: E501
    )
    default_warehouse: str = Field(..., description="The default warehouse that shall be used for any data source")
    category: Category = Field(Category.SNOWFLAKE, title="category", **{"ui": {"checkbox": False}})  # type: ignore[call-overload]

    def __init__(self, **kwargs):
        super().__init__(**{k: v for k, v in kwargs.items() if k != "secrets_keeper"})
        self.token_url = f"https://{self.account}.snowflakecomputing.com/oauth/token-request"
        self.authorization_url = f"https://{self.account}.snowflakecomputing.com/oauth/authorize"
        self._oauth2_connector = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=self.authorization_url,
            scope=self.scope,
            token_url=self.token_url,
            redirect_uri=self.redirect_uri,
            config=OAuth2ConnectorConfig(
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
            secrets_keeper=kwargs.get("secrets_keeper", None),
        )

    def build_authorization_url(self, **kwargs):
        return self._oauth2_connector.build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self._oauth2_connector.retrieve_tokens(authorization_response)

    def get_access_token(self):
        return self._oauth2_connector.get_access_token()

    def _get_connection(self, database: str | None = None, warehouse: str | None = None) -> "SnowflakeConnection":
        def connect_function() -> "SnowflakeConnection":
            _LOGGER.info("Connect at Snowflake")
            token_start = timer()
            tokens = self.get_access_token()
            token_end = timer()

            connection_params = {
                "account": self.account,
                "authenticator": AuthenticationMethod.OAUTH,
                "application": "ToucanToco",
                "token": tokens,
                "role": self.role if self.role else "",
            }
            _LOGGER.info(
                f"[benchmark][snowflake] - get_access_token {token_end - token_start} seconds",
                extra={
                    "benchmark": {
                        "operation": "get_access_token",
                        "execution_time": token_end - token_start,
                        "connector": "snowflake",
                    }
                },
            )

            _LOGGER.info(
                f"Connect at Snowflake with {connection_params}, database {database} and warehouse {warehouse}"
            )
            connect_start = timer()
            connection = snowflake.connector.connect(**connection_params, database=database, warehouse=warehouse)
            connect_end = timer()
            _LOGGER.info(
                f"[benchmark][snowflake] - connect {connect_end - connect_start} seconds",
                extra={
                    "benchmark": {
                        "operation": "connect",
                        "execution_time": connect_end - connect_start,
                        "connector": "snowflake",
                    }
                },
            )
            return connection

        def alive_function(conn: SnowflakeConnection) -> Any:
            _LOGGER.debug("Check Snowflake connection")
            if hasattr(conn, "is_closed"):
                try:
                    return not conn.is_closed()
                except Exception as exc:
                    raise TypeError("is_closed is not a function") from exc

        def close_function(conn: SnowflakeConnection) -> None:
            _LOGGER.debug("Close Snowflake connection")
            if hasattr(conn, "close"):
                try:
                    close_start = timer()
                    conn.close()
                    close_end = timer()
                    _LOGGER.info(
                        f"[benchmark][snowflake] - close {close_end - close_start} seconds",
                        extra={
                            "benchmark": {
                                "operation": "close",
                                "execution_time": close_end - close_start,
                                "connector": "snowflake",
                            }
                        },
                    )
                    return None
                except Exception as exc:
                    raise TypeError("close is not a function") from exc

        assert connection_manager is not None
        connection: SnowflakeConnection = connection_manager.get(
            identifier=f"{self.get_identifier()}{database}{warehouse}",
            connect_method=connect_function,
            alive_method=alive_function,
            close_method=close_function,
            save=True if database and warehouse else False,
        )

        return connection

    def get_identifier(self):
        json_uid = JsonWrapper.dumps(
            {
                "name": self.name,
                "account": self.account,
                "client_id": self.client_id,
                "scope": self.scope,
                "role": self.role,
            },
            sort_keys=True,
        )
        string_uid = str(uuid.uuid3(uuid.NAMESPACE_OID, json_uid))
        return string_uid

    def _get_unique_datasource_identifier(self, data_source: SnowflakeoAuth2DataSource) -> dict:
        return SnowflakeCommon().render_datasource(data_source)

    def _get_warehouses(self, warehouse_name: str | None = None) -> list[str]:
        with self._get_connection(warehouse=warehouse_name) as connection:
            result = SnowflakeCommon().get_warehouses(connection, warehouse_name)
        return result

    def _set_warehouse(self, data_source: SnowflakeoAuth2DataSource):
        warehouse = data_source.warehouse
        if self.default_warehouse and not warehouse:
            data_source.warehouse = self.default_warehouse
        return data_source

    def _get_databases(self, database_name: str | None = None) -> list[str]:
        with self._get_connection(database=database_name) as connection:
            result = SnowflakeCommon().get_databases(connection, database_name)
        return result

    def _retrieve_data(self, data_source: SnowflakeoAuth2DataSource) -> "pd.DataFrame":
        with self._get_connection(database=data_source.database, warehouse=data_source.warehouse) as connection:
            result = SnowflakeCommon().retrieve_data(connection, data_source)
        return result

    def get_slice(
        self,
        data_source: SnowflakeoAuth2DataSource,
        permissions: dict | None = None,
        offset: int = 0,
        limit: int | None = None,
        get_row_count: bool | None = False,
    ) -> DataSlice:
        with self._get_connection(database=data_source.database, warehouse=data_source.warehouse) as connection:
            result = SnowflakeCommon().get_slice(
                connection,
                data_source,
                offset=offset,
                limit=limit,
                get_row_count=bool(get_row_count),
            )
        return result

    def describe(self, data_source: SnowflakeoAuth2DataSource) -> dict[str, str]:
        data_source = self._set_warehouse(data_source)
        with self._get_connection(data_source.database, data_source.warehouse) as connection:
            result = SnowflakeCommon().describe(connection, data_source.query)
        return result

    @staticmethod
    def get_connection_manager():
        return connection_manager

    def _get_connection_and_db_content(self, database: str, db_contents: list[dict[str, Any]]):
        with self._get_connection(database=database, warehouse=self.default_warehouse) as connection:
            db_contents += SnowflakeCommon().get_db_content(connection).to_dict("records")  # type: ignore

    def get_model(
        self,
        db_name: str | None = None,
        schema_name: str | None = None,
        table_name: str | None = None,
        exclude_columns: bool = False,
    ) -> list[TableInfo]:
        with self._get_connection() as connection:
            databases = SnowflakeCommon().get_databases(connection=connection)
        content_queries = []
        for _ in databases:
            content_queries.append(build_database_model_extraction_query())
        db_contents: list[dict[str, Any]] = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._get_connection_and_db_content, db, db_contents) for db in databases]
            for future in concurrent.futures.as_completed(futures):
                if future.exception():
                    raise future.exception()  # type: ignore[misc]
                else:
                    _LOGGER.info("query finished")
        return DiscoverableConnector.format_db_model(db_contents)
