import pandas as pd
import logging


def cumsum(df, new_column, column, index, date_column, date_format):
    """
    DEPRECATED : Creates a new column, which is the cumsum of the column
    :param df: the dataframe
    :param new_column: name of the new column
    :param column: name on which the cumulative sum is performed
    :param index: array of column names to keep as indices
    :param date_column: column name that represent the date
    :param date_format: format of the date
    :return:
    """
    logging.getLogger(__name__).warning(f"DEPRECATED: use compute_cumsum")
    date_temp = '__date_temp__'
    if isinstance(index, str):
        index = [index]
    levels = list(range(0, len(index)))
    df[date_temp] = pd.to_datetime(df[date_column], format=date_format)
    reference_cols = [date_temp, date_column]
    df = df.groupby(index + reference_cols).sum()
    df[new_column] = df.groupby(level=levels)[column].cumsum()
    df.reset_index(inplace=True)
    del df[date_temp]

    return df
