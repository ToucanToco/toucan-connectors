from collections.abc import Generator
from contextlib import suppress
from datetime import datetime
from logging import getLogger
from typing import Any, Protocol, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, create_model

try:
    import pandas as pd
    from hubspot import HubSpot

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from toucan_connectors.pagination import build_pagination_info
from toucan_connectors.toucan_connector import (
    DataSlice,
    PlainJsonSecretStr,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

HUBSPOT_DEFAULT_DATASETS = ["contacts", "companies", "deals", "quotes", "owners"]


class HubspotDataSource(ToucanDataSource):
    dataset: str = Field(..., title="Dataset", description="The dataset to extract the data from")
    properties: list[str] = Field(default_factory=list, description="List of all properties you want to retrieve")

    @classmethod
    def get_form(cls, connector: "HubspotConnector", current_config, **kwargs):
        constraints: Any = {"dataset": strlist_to_enum("dataset", HUBSPOT_DEFAULT_DATASETS)}
        with suppress(Exception):
            # try to retrieve user's custom defined objects
            available_custom_objects = connector.get_custom_objects()
            constraints["dataset"] = strlist_to_enum("dataset", HUBSPOT_DEFAULT_DATASETS + available_custom_objects)
        return create_model("FormSchema", **constraints, __base__=cls).schema()


class _HubSpotPagingNext(BaseModel):
    after: str
    link: str


class _HubSpotPaging(BaseModel):
    next_: _HubSpotPagingNext = Field(..., alias="next")


class _HubSpotResult(BaseModel):
    created_at: datetime | None = None
    updated_at: datetime | None = None
    id_: str = Field(..., alias="id")
    properties: dict[str, Any] = Field(default_factory=dict)
    model_config = ConfigDict(extra="allow")

    def to_dict(self) -> dict[str, Any]:
        dict_ = self.dict(by_alias=True)
        properties = dict_.pop("properties", None)

        return {**dict_, **(properties or {})}


class _HubSpotResponse(BaseModel):
    paging: _HubSpotPaging | None = None
    results: list[_HubSpotResult]

    def next_page_after(self) -> str | None:
        return None if self.paging is None else self.paging.next_.after


_RawHubSpotResult: TypeAlias = dict[str, str | None | list[Any]]


class _HubSpotObject(Protocol):  # pragma: no cover
    def to_dict(self) -> _RawHubSpotResult: ...


def _get_all(client: "HubSpot", dataset: str) -> list[_HubSpotObject]:  # pragma: no cover
    return client.crm.objects.get_all(dataset)


def _get_page(
    client: "HubSpot", dataset: str, after: str | None, limit: int | None, properties: list[str]
) -> _HubSpotObject:  # pragma: no cover
    return client.crm.objects.basic_api.get_page(dataset, after=after, limit=limit, properties=properties)


class HubspotConnector(ToucanConnector, data_source_model=HubspotDataSource):
    access_token: PlainJsonSecretStr = Field(..., description="An API key for the target private app")

    def _fetch_page(
        self, dataset: str, properties: list[str], after: str | None = None, limit: int | None = None
    ) -> _HubSpotResponse:
        client = HubSpot(access_token=self.access_token.get_secret_value())
        page = _get_page(client=client, dataset=dataset, after=after, limit=limit, properties=properties)
        return _HubSpotResponse(**page.to_dict())

    def _fetch_all(self, dataset: str, properties: list[str]) -> list[_HubSpotResult]:
        client = HubSpot(access_token=self.access_token.get_secret_value())
        results = _get_all(client=client, dataset=dataset)
        return [_HubSpotResult(**elem.to_dict()) for elem in results]

    def _retrieve_data(self, data_source: HubspotDataSource) -> "pd.DataFrame":
        return pd.DataFrame(
            r.to_dict() for r in self._fetch_all(data_source.dataset, properties=data_source.properties)
        )

    def _result_iterator(
        self, dataset: str, properties: list[str], max_results: int | None, limit: int | None
    ) -> Generator[_HubSpotResult, None, None]:
        after = None
        count = 0
        # HubSpot returns a 400 HTTP error when trying to fetch more than 100 results
        if limit is not None:
            limit = min(limit, 100)
        while True:
            page = self._fetch_page(dataset=dataset, properties=properties, after=after, limit=limit)
            for result in page.results:
                if max_results is not None and count >= max_results:
                    return
                yield result
                count += 1

            if (after := page.next_page_after()) is None:
                return

    def get_slice(
        self,
        data_source: HubspotDataSource,
        permissions: dict | None = None,
        offset: int = 0,
        limit: int | None = None,
        get_row_count: bool | None = False,
    ) -> DataSlice:
        if limit is None:
            df = self._retrieve_data(data_source)[offset:].reset_index(drop=True)

        else:
            results = []
            result_iterator = self._result_iterator(
                dataset=data_source.dataset,
                properties=data_source.properties,
                max_results=offset + (limit or 0),
                limit=limit,
            )
            try:
                for _ in range(offset):
                    next(result_iterator)
                results = list(result_iterator)
            except StopIteration:
                pass
            df = pd.DataFrame([r.to_dict() for r in results])

        return DataSlice(
            df=df,
            pagination_info=build_pagination_info(offset=offset, limit=limit, retrieved_rows=len(df), total_rows=None),
        )

    def get_custom_objects(self) -> list[str]:  # pragma: no cover
        client = HubSpot(access_token=self.access_token.get_secret_value())
        return [obj.fully_qualified_name for obj in client.crm.schemas.core_api.get_all().results]
