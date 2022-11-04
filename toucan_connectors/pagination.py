from typing import Literal

from pydantic import BaseModel


class OffsetLimitInfo(BaseModel):
    # Both of these must be positive integers
    offset: int
    # Limit can be None because charts need to be able to retrieve all data
    limit: int | None


class UnknownSizeDatasetPaginationInfo(BaseModel):
    type: Literal['unknown_size'] = 'unknown_size'
    # Will be true if there is no more information to fetch
    is_last_page: bool


class KnownSizeDatasetPaginationInfo(BaseModel):
    type: Literal['known_size'] = 'known_size'
    # Will be true if there is no more information to fetch
    is_last_page: bool
    # the total size of the dataset
    total_rows: int


class PaginationInfo(BaseModel):
    # A recap of the provided parameters
    parameters: OffsetLimitInfo
    pagination_info: UnknownSizeDatasetPaginationInfo | KnownSizeDatasetPaginationInfo
    # The parameters to provide to the API in order to retrieve the next page.
    # If None, there is no more data left to retrieve
    next_page: OffsetLimitInfo | None
    # The parameters to provide to the API in order to retrieve the previous page.
    # If None, there is no previous data to retrieve
    previous_page: OffsetLimitInfo | None


def build_pagination_info(
    *,
    offset: int,
    limit: int | None,
    retrieved_rows: int,
    total_rows: int | None,
) -> PaginationInfo:
    # We're on the last page if:
    # * No limit was specified
    # * The total dataset size is known AND limit+offset it greater or equal to the total size
    # * We retrieved less rows than limit (meaning we've reached the end)
    is_last_page = (
        limit is None
        or (total_rows is not None and (offset + limit) >= total_rows)
        # FIXME: bogus if the last page has a length of "limit"
        or retrieved_rows < limit
    )
    if total_rows is not None:
        pagination_info = KnownSizeDatasetPaginationInfo(
            is_last_page=is_last_page, total_rows=total_rows
        )
    # If we've reached the last page AND we have at least one result, we know the size of the
    # dataset. If we had no results, we could be several rows after the actual dataset
    elif is_last_page and retrieved_rows > 0:
        pagination_info = KnownSizeDatasetPaginationInfo(
            is_last_page=True, total_rows=offset + retrieved_rows
        )
    else:
        pagination_info = UnknownSizeDatasetPaginationInfo(is_last_page=is_last_page)

    next_page = (
        OffsetLimitInfo(offset=offset + limit, limit=limit)
        if limit is not None and not is_last_page
        else None
    )
    # In case limit is None, we don't know how many rows back we need to go, so previous_page is None
    if limit is None:
        previous_page = None
    else:
        if offset > 0:
            previous_page = OffsetLimitInfo(offset=max(offset - limit or 0, 0), limit=limit)
        else:
            previous_page = None

    return PaginationInfo(
        parameters=OffsetLimitInfo(offset=offset, limit=limit),
        pagination_info=pagination_info,
        next_page=next_page,
        previous_page=previous_page,
    )
