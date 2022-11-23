from typing import Literal

from pydantic import BaseModel


class OffsetLimitInfo(BaseModel):
    """Represents offset and limit information for pagination.

    Both `offset` and `limit` must be positive integers.
    """

    offset: int
    limit: int | None


class UnknownSizeDatasetPaginationInfo(BaseModel):
    """Represents pagination information about a dataset of unknown size.

    :param is_last_page: indicates wether the last page has been reached.
    """

    type: Literal['unknown_size'] = 'unknown_size'
    is_last_page: bool


class KnownSizeDatasetPaginationInfo(BaseModel):
    """Represents pagination information about a dataset of unknown size.

    :param is_last_page: Indicates wether the last page has been reached.
    :param total_rows: The total number of rows in the dataset.
    """

    type: Literal['known_size'] = 'known_size'
    is_last_page: bool
    total_rows: int


class PaginationInfo(BaseModel):
    """Represents pagination information for a given dataset.

    :param parameters: A recap of the provided parameters.
    :param pagination_info: Contains information about where we're at.
    :param next_page: Contains the parameters to provide to the API in order to retrieve the next
                      page. If None, there is no data left to retrieve.
    :param previous_page: Contains the parameters to provide to the API in order to retrieve the
                          previous page. If None, there is no previous data to retrieve.
    """

    parameters: OffsetLimitInfo
    pagination_info: UnknownSizeDatasetPaginationInfo | KnownSizeDatasetPaginationInfo
    next_page: OffsetLimitInfo | None
    previous_page: OffsetLimitInfo | None


def build_pagination_info(
    *,
    offset: int,
    limit: int | None,
    retrieved_rows: int,
    total_rows: int | None,
) -> PaginationInfo:
    """Builds a `PaginationInfo` object based on the provided parameters.

    :param offset: The offset that was provided.
    :param limit: The limit that was provided.
    :param retrieved_rows: The number of rows that were retrieved from the backend for the
                           provided offset and limit.
    :param total_rows: The total number of rows in the dataset, if known.
    """
    # We're on the last page if:
    # * No limit was specified
    # * The total dataset size is known AND limit+offset is greater or equal to the total size
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
    if offset > 0 and limit is not None:
        previous_page = OffsetLimitInfo(offset=max(offset - limit or 0, 0), limit=limit)
    else:
        previous_page = None

    return PaginationInfo(
        parameters=OffsetLimitInfo(offset=offset, limit=limit),
        pagination_info=pagination_info,
        next_page=next_page,
        previous_page=previous_page,
    )
