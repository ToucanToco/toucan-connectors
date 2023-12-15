from toucan_connectors.pagination import (
    KnownSizeDatasetPaginationInfo,
    PaginationInfo,
    build_pagination_info,
)


def test_build_pagination_info():
    pagination_info = build_pagination_info(offset=0, limit=50, retrieved_rows=50, total_rows=96)
    assert pagination_info == PaginationInfo(
        parameters={"offset": 0, "limit": 50},
        previous_page=None,
        next_page={"offset": 50, "limit": 50},
        pagination_info=KnownSizeDatasetPaginationInfo(total_rows=96, is_last_page=False),
    )
