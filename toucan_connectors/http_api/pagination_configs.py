import logging
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from pydantic import BaseModel, Field

from toucan_connectors.common import FilterSchema
from toucan_connectors.http_api.http_api_data_souce import HttpAPIDataSource

_LOGGER = logging.getLogger(__name__)


class PaginationConfig(BaseModel):
    """Base class for pagination configs.

    Config applied for connectors without pagination configured.
    Useful for connectors that can return all results at once.
    """

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        """Apply pagination to data source and return it"""
        pass

    def get_next_pagination_config(self, result: Any, pagination_info: Any | None) -> Optional["PaginationConfig"]:
        """Computes next pagination config based on the parsed API responses"""
        pass

    def get_pagination_info_filter(self) -> str | None:
        """Returns the JQ filter that must be applied to the raw API result to retrieve pagination info"""
        pass

    def get_error_status_whitelist(self) -> list[str] | None:
        """Returns the list of the error statuses which means the end of data fetching, and so to ignore"""
        pass


class NoopPaginationConfig(PaginationConfig):
    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        return data_source

    def get_next_pagination_config(self, result: Any, pagination_info: Any | None) -> Optional["PaginationConfig"]:
        return None

    def get_pagination_info_filter(self) -> str | None:
        return None


class OffsetLimitPaginationConfig(PaginationConfig):
    offset_name: str = "offset"
    offset: int = 0
    limit_name: str = "limit"
    limit: int

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        offset_limit_params = {self.offset_name: self.offset, self.limit_name: self.limit}
        if data_source.params is None:
            data_source_params = offset_limit_params
        else:
            data_source_params = data_source.params | offset_limit_params
        return data_source.model_copy(update={"params": data_source_params})

    def get_next_pagination_config(
        self, result: Any, pagination_info: Any | None
    ) -> Optional["OffsetLimitPaginationConfig"]:
        if len(result) < self.limit:
            return None
        else:
            return self.model_copy(update={"offset": self.offset + self.limit})


class PageBasedPaginationConfig(PaginationConfig):
    page_name: str = "page"
    page: int = 0
    per_page_name: str | None = None
    per_page: int | None = None
    max_page_filter: str | None = None
    can_raise_not_found: bool = Field(
        False,
        description="Some APIs can raise a not found error (404) when requesting the next page.",
    )

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        page_based_params = {self.page_name: self.page}
        if self.per_page_name:
            page_based_params |= {self.per_page_name: self.per_page}
        if data_source.params is None:
            data_source_params = page_based_params
        else:
            data_source_params = data_source.params | page_based_params
        return data_source.model_copy(update={"params": data_source_params})

    def get_next_pagination_config(
        self, result: Any, pagination_info: Any | None
    ) -> Optional["PageBasedPaginationConfig"]:
        if self.max_page_filter:
            if pagination_info is None:
                return None
            if self.page >= int(pagination_info):
                return None
        if self.per_page:
            if len(result) < self.per_page:
                return None
        if len(result) < 1:
            return None
        else:
            return self.model_copy(update={"page": self.page + 1})

    def get_pagination_info_filter(self) -> str:
        return self.max_page_filter

    def get_error_status_whitelist(self) -> list[int] | None:
        if self.can_raise_not_found:
            return [404]
        return None


class CursorBasedPaginationConfig(PaginationConfig):
    cursor_name: str = "cursor"
    cursor: str | None = None
    cursor_filter: str = FilterSchema

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        if self.cursor:
            cursor_params = {self.cursor_name: self.cursor}
            if data_source.params is None:
                data_source_params = cursor_params
            else:
                data_source_params = data_source.params | cursor_params
            return data_source.model_copy(update={"params": data_source_params})
        else:
            return data_source

    def get_next_pagination_config(
        self, result: Any, pagination_info: Any | None
    ) -> Optional["CursorBasedPaginationConfig"]:
        if pagination_info is None:
            return None
        if isinstance(pagination_info, dict) or isinstance(pagination_info, list):
            raise ValueError(f"Invalid next cursor value. Cursor can't be a complex value, got: {pagination_info}")
        else:
            return self.model_copy(update={"cursor": str(pagination_info)})

    def get_pagination_info_filter(self) -> str:
        return self.cursor_filter


class HyperMediaPaginationConfig(PaginationConfig):
    next_link_filter: str
    next_link: str | None = None

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        if self.next_link:
            url_chunks = urlparse(self.next_link)
            url_parameters = parse_qs(url_chunks.query) | (data_source.params or {})
            return data_source.model_copy(update={"url": url_chunks.path, "params": url_parameters})
        else:
            return data_source

    def get_next_pagination_config(
        self, result: Any, pagination_info: Any | None
    ) -> Optional["HyperMediaPaginationConfig"]:
        if pagination_info is None:
            return None
        if isinstance(pagination_info, dict) or isinstance(pagination_info, list):
            raise ValueError(f"Invalid next link value. Link can't be a complex value, got: {pagination_info}")
        else:
            return self.model_copy(update={"next_link": str(pagination_info)})

    def get_pagination_info_filter(self) -> str:
        return self.next_link_filter


HttpPaginationConfig = (
    CursorBasedPaginationConfig
    | PageBasedPaginationConfig
    | OffsetLimitPaginationConfig
    | NoopPaginationConfig
    | HyperMediaPaginationConfig
)


def extract_pagination_info_from_result(api_response: dict | list, jq_pagination_filter: str):
    import jq

    try:
        return jq.first(jq_pagination_filter, api_response)
    except ValueError:
        _LOGGER.info(f"Could not extract pagination info with filter '{jq_pagination_filter}' on {api_response}")
        return None
