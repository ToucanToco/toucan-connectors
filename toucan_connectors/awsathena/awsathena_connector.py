import logging
from typing import Annotated, Any

from cached_property import cached_property_with_ttl
from pydantic import ConfigDict, Field, StringConstraints, create_model

try:
    import awswrangler as wr
    import boto3
    import pandas as pd

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    logging.getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from toucan_connectors.common import ConnectorStatus, apply_query_parameters, sanitize_query
from toucan_connectors.pagination import build_pagination_info
from toucan_connectors.pandas_translator import PandasConditionTranslator
from toucan_connectors.toucan_connector import (
    DataSlice,
    DataStats,
    DiscoverableConnector,
    PlainJsonSecretStr,
    TableInfo,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)


class AwsathenaDataSource(ToucanDataSource):
    name: str = Field(..., description="Your AWS Athena connector name")
    database: Annotated[str, StringConstraints(min_length=1)] = Field(
        ..., description="The name of the database you want to query."
    )
    # To avoid previous config migrations, won't be used
    table: str | None = Field(None, **{"ui.hidden": True})  # type: ignore[call-overload]
    language: str = Field("sql", **{"ui.hidden": True})  # type: ignore[call-overload]
    query: Annotated[str | None, StringConstraints(min_length=1)] = Field(  # type: ignore[call-overload]
        None,
        description="The SQL query to execute.",
        widget="sql",
    )
    query_object: dict | None = Field(  # type: ignore[call-overload]
        None,
        description="An object describing a simple select query This field is used internally",
        **{"ui.hidden": True},
    )
    use_ctas: bool = Field(
        False,
        description="Set to true if you want to use CTAS (recommended for big queries only)",
    )

    @classmethod
    def get_form(cls, connector: "AwsathenaConnector", current_config: dict[str, Any]):
        return create_model(
            "FormSchema",
            database=strlist_to_enum("database", connector.available_dbs),
            __base__=cls,
        ).schema()

    def __init__(self, **data):
        super().__init__(**data)
        # Named parameters need to be passed as `:name`
        # (see https://aws-data-wrangler.readthedocs.io/en/stable/stubs/awswrangler.athena.read_sql_query.html)
        self.query, self.parameters = sanitize_query(self.query, self.parameters, athena_variable_transformer)


def athena_variable_transformer(variable: str):
    """Add surrounding for parameters injection"""
    return f":{variable}"


class AwsathenaConnector(ToucanConnector, DiscoverableConnector, data_source_model=AwsathenaDataSource):
    name: str = Field(..., description="Your AWS Athena connector name")

    s3_output_bucket: str = Field(..., description="Your S3 Output bucket (where query results are stored.)")
    aws_access_key_id: str = Field(..., description="Your AWS access key ID")
    aws_secret_access_key: PlainJsonSecretStr | None = Field(None, description="Your AWS secret key")
    region_name: str = Field(..., description="Your AWS region name")
    model_config = ConfigDict(ignored_types=(cached_property_with_ttl,))

    def get_session(self) -> "boto3.Session":
        assert self.aws_secret_access_key is not None, "'aws_secret_access_key' is required"
        return boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            # This is required because this gets appended by boto3
            # internally, and a PlainJsonSecretStr can't be appended to an str
            aws_secret_access_key=self.aws_secret_access_key.get_secret_value(),
            region_name=self.region_name,
        )

    @staticmethod
    def _strip_trailing_semicolumn(query: str) -> str:
        q = query.strip()
        return q[:-1] if q.endswith(";") else q

    @classmethod
    def _add_pagination_to_query(cls, query: str, offset: int = 0, limit: int | None = None) -> str:
        if offset and limit:
            return f"SELECT * FROM ({cls._strip_trailing_semicolumn(query)}) OFFSET {offset} LIMIT {limit};"
        if limit:
            return f"SELECT * FROM ({cls._strip_trailing_semicolumn(query)}) LIMIT {limit};"
        return query

    @cached_property_with_ttl(ttl=10)
    def available_dbs(self) -> list[str]:
        return self._list_db_names()

    def _retrieve_data(
        self,
        data_source: AwsathenaDataSource,
        offset: int = 0,
        limit: int | None = None,
    ) -> "pd.DataFrame":
        assert data_source.query is not None, "no query provided"
        df = wr.athena.read_sql_query(
            self._add_pagination_to_query(
                data_source.query,
                offset=offset,
                limit=limit,
            ),
            params=data_source.parameters,
            database=data_source.database,
            boto3_session=self.get_session(),
            s3_output=self.s3_output_bucket,
            ctas_approach=data_source.use_ctas,
            paramstyle="named",
        )
        return df

    def _list_db_names(self) -> list[str]:
        return [
            str(value)
            for value in wr.catalog.databases(
                boto3_session=self.get_session(),
            )["Database"].values.tolist()
        ]

    def _get_project_structure(self, db_name: str | None = None) -> list[TableInfo]:
        table_list: list[TableInfo] = []
        available_dbs = self.available_dbs if db_name is None else [db_name]
        session = self.get_session()
        for db in available_dbs:
            tables = wr.catalog.tables(boto3_session=session, database=db)[["Table", "TableType"]].to_dict(
                orient="records"
            )
            for table_object in tables:
                if "temp_table" not in table_object["Table"]:
                    columns = (
                        wr.catalog.get_table_types(boto3_session=session, database=db, table=table_object["Table"])
                        or {}
                    )
                    table_list.append(
                        {
                            "name": table_object["Table"],
                            "database": db,
                            "type": "table" if "TABLE" in table_object["TableType"] else "view",
                            "columns": [{"name": k, "type": v} for k, v in columns.items()],
                        }
                    )
        return table_list

    def get_slice(
        self,
        data_source: AwsathenaDataSource,
        permissions: dict | None = None,
        offset: int = 0,
        limit: int | None = None,
        get_row_count: bool | None = False,
    ) -> DataSlice:
        df = self._retrieve_data(data_source, offset=offset, limit=limit)
        df.columns = df.columns.astype(str)

        if permissions is not None:
            permissions_query = PandasConditionTranslator.translate(permissions)
            permissions_query = apply_query_parameters(permissions_query, data_source.parameters or {})
            df = df.query(permissions_query)

        return DataSlice(
            df,
            stats=DataStats(df_memory_size=df.memory_usage().sum()),
            pagination_info=build_pagination_info(offset=offset, limit=limit, retrieved_rows=len(df), total_rows=None),
        )

    def get_status(self) -> ConnectorStatus:
        checks = [
            "Host resolved",
            "Port opened",
            "Connected",
            "Authenticated",
            "Can list databases",
        ]
        session = self.get_session()
        try:
            # Returns a pandas DataFrame of DBs
            wr.catalog.databases(boto3_session=session)
            return ConnectorStatus(status=True, details=[(c, True) for c in checks], error=None)
        # aws-wrangler exceptions all inherit Exception directly:
        # https://github.com/awslabs/aws-data-wrangler/blob/main/awswrangler/exceptions.py
        except Exception as exc:
            try:
                sts_client = session.client("sts")
                sts_client.get_caller_identity()
                # We consider an authenticated client capable of
                # connecting to AWS to be valid, even if sub-optimal
                return ConnectorStatus(
                    status=True,
                    details=[(c, i < 4) for (i, c) in enumerate(checks)],
                    error=f"Cannot list databases: {exc}",
                )
            except Exception as sts_exc:
                # Cannot list databases nor get identity
                return ConnectorStatus(
                    status=False,
                    details=[(c, False) for c in checks],
                    error=f"Cannot verify connection to Athena: {exc}, {sts_exc}",
                )

    def get_model(
        self,
        db_name: str | None = None,
        schema_name: str | None = None,
        table_name: str | None = None,
        exclude_columns: bool = False,
    ) -> list[TableInfo]:
        """Retrieves the database tree structure using current session"""
        return self._get_project_structure(db_name)
