from collections.abc import Generator
from contextlib import contextmanager
from functools import _lru_cache_wrapper, cached_property, lru_cache
from typing import Any, List, Optional, Pattern, Union
from warnings import warn

import pandas as pd
import pymongo
from bson.son import SON
from pydantic import ConfigDict, Field, create_model, field_validator, model_validator

from toucan_connectors.common import ConnectorStatus, nosql_apply_parameters_to_query
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.mongo.mongo_translator import MongoConditionTranslator
from toucan_connectors.pagination import build_pagination_info
from toucan_connectors.toucan_connector import (
    DataSlice,
    PlainJsonSecretStr,
    ToucanConnector,
    ToucanDataSource,
    UnavailableVersion,
    VersionableEngineConnector,
    decorate_func_with_retry,
    strlist_to_enum,
)

MAX_COUNTED_ROWS = 1000001


def _is_empty_match_column(elem: Any):
    # Not matching None here, because we don't want to consider {'$match': {'col': None}} to be
    # empty
    if elem == {}:
        return True
    if isinstance(elem, dict):
        return _is_match_empty(elem)

    return False


def _is_match_empty(match_: dict) -> bool:
    return all(_is_empty_match_column(v) for v in match_.values())


def _is_match_statement(d: Any) -> bool:
    return isinstance(d, dict) and list(d.keys()) == ["$match"]


def _sanitize_match(query: dict) -> dict:
    if _is_match_empty(query):
        return {}
    if "$and" in query:
        and_condition = query["$and"]
        if isinstance(and_condition, list):
            query["$and"] = [elem for elem in and_condition if not _is_empty_match_column(elem)]
    return query


def _sanitize_query_matches(query: dict | list[dict]) -> Any:
    """Transforms match operations matching nothing into match-alls.

    If a $match would match nothing (for example, {'$match': {'field': {}}}), transform into a
    passthrough. It cannot be removed from the query to prevent having an empty query.
    """
    if isinstance(query, list):
        return [{"$match": _sanitize_match(q["$match"])} if _is_match_statement(q) else q for q in query]
    return query


def normalize_query(query, parameters):
    query = nosql_apply_parameters_to_query(query, parameters)

    # FIXME: This removes empty $match operations that could have been created while removing
    # missing parameters from the query in nosql_apply_parameters to query. However, this way of
    # handling __VOID__ filters is hacky and should be implemented earlier, when translating the VQB
    # pipeline
    query = _sanitize_query_matches(query)

    if isinstance(query, dict):
        query = [{"$match": query}]

    for stage in query:
        # Allow ordered sorts
        if "$sort" in stage and isinstance(stage["$sort"], list):
            stage["$sort"] = SON([x.popitem() for x in stage["$sort"]])

    return query


def apply_condition_filter(query, permissions_condition: dict):
    if permissions_condition:
        permissions = MongoConditionTranslator.translate(permissions_condition)
        if isinstance(query, dict):
            query = {"$and": [query, permissions]}
        else:
            query[0]["$match"] = {"$and": [query[0]["$match"], permissions]}
    return query


def validate_database(client: pymongo.MongoClient, database: str):
    if database not in client.list_database_names():
        raise UnkwownMongoDatabase(f"Database {database!r} doesn't exist")


def validate_collection(client, database: str, collection: str):
    if collection not in client[database].list_collection_names():
        raise UnkwownMongoCollection(f"Collection {collection!r} doesn't exist")


class MongoDataSource(ToucanDataSource):
    database: str = Field(..., description="The name of the database you want to query")
    collection: str = Field(..., description="The name of the collection you want to query")
    query: Union[dict, list] = Field(
        {},
        description="A mongo query. See more details on the Mongo " "Aggregation Pipeline in the MongoDB documentation",
    )

    # FIXME: This is needed for now because with we rely on empty queries being dicts. In pydantic
    # v1, "[]" was coerced to {}, and we somehow rely on that cursed behaviour
    @field_validator("query")
    @classmethod
    def _ensure_empty_query_is_dict(cls, query: dict | list) -> dict | list:
        return query or {}

    @classmethod
    def get_form(cls, connector: "MongoConnector", current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `collection` field
        """
        constraints = {}

        # Always add the suggestions for the available databases
        with connector.client() as client:
            available_databases = client.list_database_names()
            constraints["database"] = strlist_to_enum("database", available_databases)

            if "database" in current_config:
                validate_database(client, current_config["database"])
                available_cols = client[current_config["database"]].list_collection_names()
                constraints["collection"] = strlist_to_enum("collection", available_cols)

        return create_model("FormSchema", __base__=cls, **constraints).schema()  # type: ignore[call-overload]


class MongoConnector(ToucanConnector, VersionableEngineConnector, data_source_model=MongoDataSource):
    """Retrieve data from a [MongoDB](https://www.mongodb.com/) database."""

    host: str = Field(
        ...,
        description="The domain name (preferred option as more dynamic) or "
        "the hardcoded IP address of your database server",
    )
    port: Optional[int] = Field(None, description="The listening port of your database server")
    username: Optional[str] = Field(None, description="Your login username")
    password: Optional[PlainJsonSecretStr] = Field(None, description="Your login password")
    ssl: Optional[bool] = Field(None, description="Create the connection to the server using SSL")
    model_config = ConfigDict(ignored_types=(cached_property, _lru_cache_wrapper))
    max_pool_size: int = Field(1, alias="maxPoolSize")

    @model_validator(mode="after")
    def password_must_have_a_user(self) -> "MongoConnector":
        if self.password is not None and self.username is None:
            raise ValueError("username must be set")
        return self

    def __hash__(self):
        return hash(id(self)) + hash(JsonWrapper.dumps(self._get_mongo_client_kwargs()))

    def __enter__(self):
        warn("Using MongoConnector as a context manager is deprecated", stacklevel=2)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = ["Hostname resolved", "Port opened", "Host connection", "Authenticated"]
        ok_checks = [(c, True) for i, c in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(c, None) for i, c in enumerate(checks) if i > index]
        return ok_checks + [new_check] + not_validated_checks

    def _get_mongo_client_kwargs(self) -> dict[str, Any]:
        # We don't want parent class attributes nor the `client` property
        # nor attributes with `None` value
        to_exclude = set(ToucanConnector.model_fields.keys()) | {"client", "max_pool_size"}
        mongo_client_kwargs = self.model_dump(exclude=to_exclude, exclude_none=True).copy()

        if "password" in mongo_client_kwargs:
            mongo_client_kwargs["password"] = mongo_client_kwargs["password"].get_secret_value()

        mongo_client_kwargs["maxPoolSize"] = self.max_pool_size

        return mongo_client_kwargs

    def get_status(self) -> ConnectorStatus:
        if self.port:
            # Check hostname
            try:
                self.check_hostname(self.host)
            except Exception as e:
                return ConnectorStatus(status=False, details=self._get_details(0, False), error=str(e))

            # Check port
            try:
                self.check_port(self.host, self.port)
            except Exception as e:
                return ConnectorStatus(status=False, details=self._get_details(1, False), error=str(e))

        # Check databases access
        mongo_client_kwargs = self._get_mongo_client_kwargs()
        mongo_client_kwargs["serverSelectionTimeoutMS"] = 500
        with self.client(mongo_client_kwargs) as client:
            try:
                client.server_info()
            except pymongo.errors.ServerSelectionTimeoutError as e:
                return ConnectorStatus(status=False, details=self._get_details(2, False), error=str(e))
            except pymongo.errors.OperationFailure as e:
                return ConnectorStatus(status=False, details=self._get_details(3, False), error=str(e))

        return ConnectorStatus(status=True, details=self._get_details(3, True), error=None)

    @contextmanager
    def client(self, client_args: dict[str, Any] | None = None) -> Generator[pymongo.MongoClient, None, None]:
        client: pymongo.MongoClient = pymongo.MongoClient(
            **(self._get_mongo_client_kwargs() if client_args is None else client_args)
        )
        try:
            yield client
        finally:
            client.close()

    @lru_cache(maxsize=32)  # noqa: B019
    def _validate_database(self, client: pymongo.MongoClient, database: str):
        return validate_database(client, database)

    @lru_cache(maxsize=32)  # noqa: B019
    def _validate_collection(self, client: pymongo.MongoClient, database: str, collection: str):
        return validate_collection(client, database, collection)

    def validate_database_and_collection(self, client: pymongo.MongoClient, database: str, collection: str):
        self._validate_database(client, database)
        self._validate_collection(client, database, collection)

    def _execute_query(self, data_source: MongoDataSource):
        with self.client() as client:
            self.validate_database_and_collection(client, data_source.database, data_source.collection)
            col = client[data_source.database][data_source.collection]
            return col.aggregate(data_source.query)  # type: ignore[arg-type]

    def _retrieve_data(self, data_source):
        data_source.query = normalize_query(data_source.query, data_source.parameters)
        data = self._execute_query(data_source)
        return pd.DataFrame(list(data))

    @decorate_func_with_retry
    def get_df(self, data_source, permissions=None):
        data_source.query = apply_condition_filter(data_source.query, permissions)
        return self._retrieve_data(data_source)

    @decorate_func_with_retry
    def get_slice(
        self,
        data_source: MongoDataSource,
        permissions: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int | None = None,
        get_row_count: bool | None = False,
    ) -> DataSlice:
        # Create a copy in order to keep the original (deepcopy-like)
        data_source = data_source.model_copy(deep=True)
        if offset or limit is not None:
            data_source.query = apply_condition_filter(data_source.query, permissions or {})
            data_source.query = normalize_query(data_source.query, data_source.parameters)

            df_facet: list[dict[str, Any]] = []
            if offset:
                df_facet.append({"$skip": offset})
            if limit is not None:
                df_facet.append({"$limit": limit})

            df_facet.append({"$unset": ["_id"]})

            facet = {
                "$facet": {
                    # counting more than 1M values can be really slow, and the exact number is not that much relevant
                    "count": [
                        {"$limit": MAX_COUNTED_ROWS},
                        {"$count": "value"},
                        {"$unset": ["_id"]},
                    ],
                    "df": df_facet,  # df_facet is never empty
                }
            }
            data_source.query.append(facet)  # type:ignore[union-attr]

            res = self._execute_query(data_source).next()
            total_count = res["count"][0]["value"] if len(res["count"]) > 0 else 0
            df = pd.DataFrame(res["df"])
        else:
            df = self.get_df(data_source, permissions)
            total_count = len(df)
            # We try to remove the _id from this DataFrame if there is one
            # ugly for now but we need to handle that in this else case
            try:
                df.pop("_id")
            except Exception:  # noqa: S110
                pass
        return DataSlice(
            df,
            pagination_info=build_pagination_info(
                offset=offset, limit=limit, retrieved_rows=len(df), total_rows=total_count
            ),
        )

    def get_slice_with_regex(
        self,
        data_source: MongoDataSource,
        search: dict[str, List[dict[str, Pattern]]],
        permissions: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> DataSlice:
        # Create a copy in order to keep the original (deepcopy-like)
        data_source = data_source.model_copy(deep=True)
        data_source.query = normalize_query(data_source.query, data_source.parameters)
        # We simply append the match regex at the end of the query,
        # Mongo will then optimize the pipeline to move the match regex to its most convenient position
        # (c.f https://docs.mongodb.com/manual/core/aggregation-pipeline-optimization/#pipeline-sequence-optimization)
        # Since Mongo '$regex' operator doesn't work with integer values, we need to check the stringified versions
        search_steps: dict[str, Any] = {}
        for condition in search:
            search_steps[f"${condition}"] = []  # convert "and"/"or" to "$and"/"$or"
            for column in search[condition]:
                search_steps[f"${condition}"].append({"$and": []})  # makes an "and" of all columns searches
                for col, regex in column.items():
                    search_steps[f"${condition}"][-1]["$and"].append(
                        {
                            "$regexMatch": {
                                "input": {"$toString": f"${col}"},
                                "regex": regex.pattern,
                                "options": "i",  # i -> Case insensitivity
                            }
                        }
                    )
        data_source.query.append({"$match": {"$expr": search_steps}})  # type:ignore[union-attr]
        data_source.query.append({"$unset": ["_id"]})  # type:ignore[union-attr]

        return self.get_slice(data_source, permissions, limit=limit, offset=offset or 0)

    def get_df_with_regex(
        self,
        data_source: MongoDataSource,
        search: dict[str, List[dict[str, Pattern]]],
        permissions: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> pd.DataFrame:
        return self.get_slice_with_regex(
            data_source=data_source,
            search=search,
            permissions=permissions,
            limit=limit,
            offset=offset,
        ).df

    @decorate_func_with_retry
    def explain(self, data_source, permissions=None):
        with self.client() as client:
            self.validate_database_and_collection(client, data_source.database, data_source.collection)
            data_source.query = apply_condition_filter(data_source.query, permissions)
            data_source.query = normalize_query(data_source.query, data_source.parameters)

            agg_cmd = SON(
                [
                    ("aggregate", data_source.collection),
                    ("pipeline", data_source.query),
                    ("cursor", {}),
                ]
            )
            result = client[data_source.database].command(command="explain", value=agg_cmd, verbosity="executionStats")
        return _format_explain_result(result)

    def get_unique_identifier(self) -> str:
        return self.json(exclude={"client"})  # client is a MongoClient instance, not json serializable

    def _get_unique_datasource_identifier(self, data_source: MongoDataSource) -> dict:
        # let's make a copy first
        data_source_rendered = data_source.model_copy(deep=True)
        data_source_rendered.query = normalize_query(data_source.query, data_source.parameters)
        return data_source_rendered.model_dump(exclude={"parameters"})

    def get_engine_version(self) -> tuple:
        try:
            with self.client() as client:
                version = client.server_info()["version"]
            return super()._format_version(version)
        except (TypeError, KeyError) as exc:
            raise UnavailableVersion from exc


def _format_explain_result(explain_result):
    """format output of an `explain` mongo command

    Return a dictionary with 2 properties:

    - 'details': the origin explain result without the `serverInfo` part
      to avoid leaing mongo server version number
    - 'summary': the list of execution statistics (i.e. drop the details of
       candidate plans)

    if `explain_result` is empty, return `None`
    """
    if explain_result:
        explain_result.pop("serverInfo", None)
        if "stages" in explain_result:
            stats = [stage["$cursor"]["executionStats"] for stage in explain_result["stages"] if "$cursor" in stage]
        else:
            stats = [explain_result["executionStats"]]
        return {
            "details": explain_result,
            "summary": stats,
        }
    return None


class UnkwownMongoDatabase(Exception):
    """raised when a database does not exist"""


class UnkwownMongoCollection(Exception):
    """raised when a collection is not in the database"""
