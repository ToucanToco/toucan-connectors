from contextlib import suppress
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def is_datetime_col(col: "pd.Series") -> bool:
    from pandas.api.types import is_datetime64_any_dtype as pd_is_datetime

    return pd_is_datetime(col) or all(isinstance(val, date) for val in col)


def sanitize_df_dates(df: "pd.DataFrame") -> "pd.DataFrame":
    """Converts all datetime columns to pd.datetime64"""
    import pandas as pd

    for col in df.columns:
        if is_datetime_col(df[col]):
            with suppress(Exception):
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce").dt.tz_localize(
                    None  # we don't want timezones in datetime series returned by connectors
                )

    return df
