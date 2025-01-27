import logging
from abc import ABC, abstractmethod
from typing import Any, Literal, Optional
from urllib.parse import parse_qs, urlparse

from pydantic import BaseModel, Field

from toucan_connectors.common import UI_HIDDEN, FilterSchemaDescription

_LOGGER = logging.getLogger(__name__)


class PaginationConfig(BaseModel, ABC):
    """Base class for pagination configs"""

    @abstractmethod
    def plan_pagination_updates_to_data_source(self, request_params: dict[str, Any] | None) -> dict[str, Any]:
        """Plans pagination updates for data source"""

    @abstractmethod
    def get_next_pagination_config(self, result: Any, pagination_info: Any | None) -> Optional["PaginationConfig"]:
        """Computes next pagination config based on the parsed API responses"""

    @abstractmethod
    def get_pagination_info_filter(self) -> str | None:
        """Returns the JQ filter that must be applied to the raw API result to retrieve pagination info"""

    @abstractmethod
    def get_error_status_whitelist(self) -> list[int] | None:
        """Returns the list of the error statuses which means the end of data fetching, and so to ignore"""


class NoopPaginationConfig(PaginationConfig):
    """Pagination config without effects

    Config applied for connectors without pagination configured.
    Useful for connectors that can return all results at once.
    """

    def plan_pagination_updates_to_data_source(self, request_params: dict[str, Any] | None) -> dict[str, Any]:
        return {}

    def get_next_pagination_config(self, result: Any, pagination_info: Any | None) -> Optional["PaginationConfig"]:
        return None

    def get_pagination_info_filter(self) -> str | None:
        return None

    def get_error_status_whitelist(self) -> list[int] | None:
        return None


class OffsetLimitPaginationConfig(PaginationConfig):
    kind: Literal["OffsetLimitPaginationConfig"] = Field(..., **UI_HIDDEN)
    offset_name: str = "offset"
    offset: int = Field(0, **UI_HIDDEN)
    limit_name: str = "limit"
    limit: int
    data_filter: str = Field(
        ".",
        description=(
            "Filter to access the received data. Allows to compare its length to the limit value. "
            "It must point to a list of results. " + FilterSchemaDescription
        ),
    )

    def plan_pagination_updates_to_data_source(self, request_params: dict[str, Any] | None) -> dict[str, Any]:
        offset_limit_params = {self.offset_name: self.offset, self.limit_name: self.limit}
        if request_params is None:
            data_source_params = offset_limit_params
        else:
            data_source_params = request_params | offset_limit_params
        return {"params": data_source_params}

    def get_next_pagination_config(
        self, result: Any, pagination_info: Any | None
    ) -> Optional["OffsetLimitPaginationConfig"]:
        if not isinstance(pagination_info, list):
            return None
        if len(pagination_info) < self.limit:
            return None
        else:
            return self.model_copy(update={"offset": self.offset + self.limit})

    def get_error_status_whitelist(self) -> list[int] | None:
        return None

    def get_pagination_info_filter(self) -> str | None:
        return self.data_filter


class PageBasedPaginationConfig(PaginationConfig):
    kind: Literal["PageBasedPaginationConfig"] = Field(..., **UI_HIDDEN)
    page_name: str = "page"
    page: int = 0
    per_page_name: str | None = None
    per_page: int | None = None
    max_page_filter: str | None = Field(None, description=FilterSchemaDescription)
    can_raise_not_found: bool = Field(
        False,
        description="Some APIs can raise a not found error (404) when requesting the next page.",
    )

    def plan_pagination_updates_to_data_source(self, request_params: dict[str, Any] | None) -> dict[str, Any]:
        page_based_params = {self.page_name: self.page}
        if self.per_page_name and self.per_page:
            page_based_params |= {self.per_page_name: self.per_page}
        if request_params is None:
            data_source_params = page_based_params
        else:
            data_source_params = request_params | page_based_params
        return {"params": data_source_params}

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

    def get_pagination_info_filter(self) -> str | None:
        return self.max_page_filter

    def get_error_status_whitelist(self) -> list[int] | None:
        if self.can_raise_not_found:
            return [404]
        return None


class CursorBasedPaginationConfig(PaginationConfig):
    kind: Literal["CursorBasedPaginationConfig"] = Field(..., **UI_HIDDEN)
    cursor_name: str = "cursor"
    cursor: str | None = Field(None, **UI_HIDDEN)
    cursor_filter: str = Field(..., description=FilterSchemaDescription)

    def plan_pagination_updates_to_data_source(self, request_params: dict[str, Any] | None) -> dict[str, Any]:
        if self.cursor:
            cursor_params = {self.cursor_name: self.cursor}
            if request_params is None:
                data_source_params = cursor_params
            else:
                data_source_params = request_params | cursor_params
            return {"params": data_source_params}
        else:
            return {}

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

    def get_error_status_whitelist(self) -> list[int] | None:
        return None


class HyperMediaPaginationConfig(PaginationConfig):
    kind: Literal["HyperMediaPaginationConfig"] = Field(..., **UI_HIDDEN)
    next_link_filter: str = Field(..., description=FilterSchemaDescription)
    next_link: str | None = None

    def plan_pagination_updates_to_data_source(self, request_params: dict[str, Any] | None) -> dict[str, Any]:
        if self.next_link:
            url_chunks = urlparse(self.next_link)
            url_parameters = parse_qs(url_chunks.query) | (request_params or {})
            return {"url": url_chunks.path, "params": url_parameters}
        else:
            return {}

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

    def get_error_status_whitelist(self) -> list[int] | None:
        return None


HttpPaginationConfig = (
    CursorBasedPaginationConfig | PageBasedPaginationConfig | OffsetLimitPaginationConfig | HyperMediaPaginationConfig
)


def extract_pagination_info_from_result(api_response: dict | list, jq_pagination_filter: str):
    import jq

    try:
        return jq.first(jq_pagination_filter, api_response)
    except ValueError:
        _LOGGER.info(f"Could not extract pagination info with filter '{jq_pagination_filter}' on {api_response}")
        return None
