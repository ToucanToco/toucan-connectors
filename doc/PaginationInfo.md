# `PaginationInfo` object

`PaginationInfo` instances contain all possible information about the requested dataset. They have the following
attributes:

* `parameters`: A recap of the provided `offset` and `limit`.
* `pagination_info`: An object containing information about the requested dataset. It can be one of the two following objects:
    * `KnownSizeDatasetPaginationInfo`: For datasets whose total size is known. Made up of the following fields:
        * `type`: Set to `"known_size"`.
        * `is_last_page`: A boolean indicating wether the last page has been reached.
        * `total_rows`: The total number of rows in the dataset.
    * `UnknownSizeDatasetPaginationInfo`: For datasets whose total size is not known. Made up of the following fields:
        * `type`: Set to `"unknown_size"`.
        * `is_last_page`: A boolean indicating wether the last page has been reached.
    * `next_page`: If present, contains the `offset` and `limit` parameters to provide to `get_slice` in order to fetch the
      next page. If absent, this is the last page.
    * `previous_page`: If present, contains the `offset` and `limit` parameters to provide to `get_slice` in order to fetch the
      previous page. If absent, this is the first page.

## Source

The source for the models is available [in the code](../toucan_connectors/pagination.py).
