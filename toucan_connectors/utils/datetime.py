from datetime import date

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as pd_is_datetime


def is_datetime_col(col: pd.Series) -> bool:
    return pd_is_datetime(col) or all(isinstance(val, date) for val in col)


def sanitize_df_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Converts all datetime columns to pd.datetime64"""
    for col in df.columns:
        if is_datetime_col(df[col]):
            df[col] = pd.to_datetime(df[col], utc=True)

    return df
