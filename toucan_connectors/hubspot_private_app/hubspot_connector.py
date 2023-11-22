from datetime import datetime
from enum import Enum
from typing import Any, Generator, Protocol, TypeAlias

import pandas as pd
from hubspot import HubSpot  # type:ignore[import-untyped]
from pydantic import BaseModel, Field, SecretStr

from toucan_connectors.pagination import build_pagination_info
from toucan_connectors.toucan_connector import DataSlice, ToucanConnector, ToucanDataSource


class HubspotDataset(str, Enum):
    contacts = 'contacts'
    companies = 'companies'
    deals = 'deals'
    quotes = 'quotes'
    owners = 'owners'


class HubspotDataSource(ToucanDataSource):
    dataset: HubspotDataset


class _HubSpotPagingNext(BaseModel):
    after: str
    link: str


class _HubSpotPaging(BaseModel):
    next_: _HubSpotPagingNext = Field(..., alias='next')


class _HubSpotResult(BaseModel):
    created_at: datetime | None
    updated_at: datetime | None
    id_: str = Field(..., alias='id')
    properties: dict[str, Any] = Field(default_factory=dict)

    # For basic_api objects, properties are in a 'properties' object. For specialized APIs, such as
    # owners, they're keys of the root object
    class Config:
        extra = 'allow'

    def to_dict(self) -> dict[str, Any]:
        dict_ = self.dict(by_alias=True)
        properties = dict_.pop('properties', None)

        return {**dict_, **(properties or {})}


class _HubSpotResponse(BaseModel):
    paging: _HubSpotPaging | None = None
    results: list[_HubSpotResult]

    def next_page_after(self) -> str | None:
        return None if self.paging is None else self.paging.next_.after


_RawHubSpotResult: TypeAlias = dict[str, str | None | list[Any]]


class _HubSpotObject(Protocol):  # pragma: no cover
    def to_dict(self) -> _RawHubSpotResult:
        ...


class _PageApi(Protocol):  # pragma: no cover
    def get_page(self, after: str | None, limit: int | None) -> _HubSpotObject:
        ...


class _Api(Protocol):  # pragma: no cover
    @property
    def basic_api(self) -> _PageApi:
        ...

    def get_all(self) -> list[_HubSpotObject]:
        ...


def _page_api_for(api: _Api, dataset: HubspotDataset) -> _PageApi:
    """Some clients have a '{name}_api' attribute.

    basic_api seems to be the default fallback
    """
    page_api_name = dataset.value + '_api'
    if hasattr(api, page_api_name):
        return getattr(api, page_api_name)
    return api.basic_api


class HubspotConnector(ToucanConnector):
    data_source_model: HubspotDataSource
    access_token: SecretStr = Field(..., description='An API key for the target private app')

    def _fetch_page(
        self, api: _PageApi, after: str | None = None, limit: int | None = None
    ) -> _HubSpotResponse:
        page = api.get_page(after=after, limit=limit)
        return _HubSpotResponse(**page.to_dict())

    def _fetch_all(self, api: _Api) -> list[_HubSpotResult]:
        results = api.get_all()
        return [_HubSpotResult(**elem.to_dict()) for elem in results]

    def _api_for_dataset(self, object_type: HubspotDataset) -> _Api:  # pragma: no cover
        client = HubSpot(access_token=self.access_token.get_secret_value())
        return getattr(client.crm, object_type.value)

    def _retrieve_data(self, data_source: HubspotDataSource) -> pd.DataFrame:
        api = self._api_for_dataset(data_source.dataset)
        return pd.DataFrame(r.to_dict() for r in self._fetch_all(api))

    def _result_iterator(
        self, api: _PageApi, max_results: int | None, limit: int | None
    ) -> Generator[_HubSpotResult, None, None]:
        after = None
        count = 0
        # HubSpot returns a 400 HTTP error when trying to fetch more than 100 results
        if limit is not None:
            limit = min(limit, 100)
        while True:
            page = self._fetch_page(api, after=after, limit=limit)
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
            api = self._api_for_dataset(data_source.dataset)
            page_api = _page_api_for(api, data_source.dataset)
            results = []
            result_iterator = self._result_iterator(
                page_api, max_results=offset + (limit or 0), limit=limit
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
            pagination_info=build_pagination_info(
                offset=offset, limit=limit, retrieved_rows=len(df), total_rows=None
            ),
        )
