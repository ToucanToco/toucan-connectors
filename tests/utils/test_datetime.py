from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from dateutil import tz
from numpy import dtype

from toucan_connectors.utils.datetime import is_datetime_col, sanitize_df_dates

DTYPE_DATETIME_WITHOUT_TIMEZONE = dtype("<M8[ns]")


@pytest.mark.parametrize(
    "col",
    [
        pd.date_range(start="2022-07-01", end="2022-08-01").to_series(),
        pd.Series([date(2022, 7, x) for x in range(1, 31)]),
        pd.Series([datetime(2022, 7, x) for x in range(1, 31)]),
    ],
)
def test_is_datetime_col_valid(col: pd.Series):
    assert is_datetime_col(col)


@pytest.mark.parametrize(
    "col",
    [
        pd.Series([*(date(2022, 7, x) for x in range(1, 31)), 42]),
        pd.Series([*(date(2022, 7, x) for x in range(1, 31)), 42]),
        pd.Series(["2022-03-05T00:02:03"]),
        pd.Series(["2022-03-05"]),
    ],
)
def test_is_datetime_col_invalid(col: pd.Series):
    assert not is_datetime_col(col)


def test_sanitize_df_dates_with_dates():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [date(2022, 7, 21), date(2022, 7, 22), date(2022, 7, 23)],
        }
    )
    # First, ensure the dtypes are invalid
    assert df.dtypes.to_list() == [dtype("int64"), dtype("object"), dtype("object")]

    assert sanitize_df_dates(df).dtypes.to_list() == [
        dtype("int64"),
        dtype("object"),
        DTYPE_DATETIME_WITHOUT_TIMEZONE,
    ]


def test_sanitize_df_dates_with_datetimes():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [datetime(2022, 7, 21), datetime(2022, 7, 22), datetime(2022, 7, 23)],
        }
    )
    assert df.dtypes.to_list() == [dtype("int64"), dtype("object"), dtype("datetime64[ns]")]
    assert sanitize_df_dates(df).dtypes.to_list() == [
        dtype("int64"),
        dtype("object"),
        DTYPE_DATETIME_WITHOUT_TIMEZONE,
    ]


def test_sanitize_df_dates_with_tz_aware_datetimes():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [
                datetime(2022, 7, 21, tzinfo=ZoneInfo("Europe/Paris")),
                datetime(2022, 7, 22, tzinfo=tz.tzoffset("Europe/Paris", 3600)),
                datetime(2022, 7, 22, tzinfo=UTC),
            ],
        }
    )
    assert df.dtypes.to_list() == [dtype("int64"), dtype("object"), dtype("object")]
    assert sanitize_df_dates(df).dtypes.to_list() == [
        dtype("int64"),
        dtype("object"),
        DTYPE_DATETIME_WITHOUT_TIMEZONE,
    ]
