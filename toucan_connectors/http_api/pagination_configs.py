from typing import Any, Optional

from pydantic import BaseModel

from toucan_connectors.common import FilterSchema
from toucan_connectors.http_api.http_api_data_souce import HttpAPIDataSource


class PaginationConfig(BaseModel):
    """Base class for pagination configs.

    Config applied for connectors without pagination configured.
    Useful for connectors that can return all results at once.
    """
    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        """Apply pagination to data source and return it"""
        return data_source

    def get_next_pagination_config(self, result: Any, pagination_info: Any | None) -> Optional["PaginationConfig"]:
        """Computes next pagination config based on the parsed API responses"""
        return None

    def get_pagination_info_filter(self) -> str | None:
        """Returns the JQ filter that must be applied to the raw API result to retrieve pagination info"""
        return None


class OffsetLimitPaginationConfig(PaginationConfig):
    offset_name: str = "offset"
    offset: int = 0
    limit_name: str = "limit"
    limit: int

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        offset_limit_params = {
            self.offset_name: self.offset,
            self.limit_name: self.limit
        }
        if data_source.params is None:
            data_source.params = offset_limit_params
        else:
            data_source.params |= offset_limit_params
        return data_source

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

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        page_based_params = {self.page_name: self.page}
        if data_source.params is None:
            data_source.params = page_based_params
        else:
            data_source.params |= page_based_params
        return data_source

    def get_next_pagination_config(
        self, result: Any, pagination_info: Any | None
    ) -> Optional["PageBasedPaginationConfig"]:
        if len(result) < 1:
            return None
        else:
            return self.model_copy(update={"page": self.page + 1})


class CursorBasedPaginationConfig(PaginationConfig):
    cursor_name: str = "cursor"
    cursor: str | None = None
    cursor_filter: str = FilterSchema

    def apply_pagination_to_data_source(self, data_source: HttpAPIDataSource) -> HttpAPIDataSource:
        if self.cursor:
            cursor_params = {self.cursor_name: self.cursor}
            if data_source.params is None:
                data_source.params = cursor_params
            else:
                data_source.params |= cursor_params
        return data_source

    def get_next_pagination_config(
        self, result: Any, pagination_info: Any | None
    ) -> Optional["CursorBasedPaginationConfig"]:
        if pagination_info is None:
            return None
        if isinstance(pagination_info, str):
            return self.model_copy(update={"cursor": pagination_info})
        else:
            raise ValueError(f"Invalid next cursor value. Cursor value must be a string, got: {pagination_info}")

    def get_pagination_info_filter(self) -> str:
        return self.cursor_filter
