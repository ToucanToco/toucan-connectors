import logging
import numpy as np
import pandas as pd

from toucan_data_sdk.utils.helpers import check_params_columns_duplicate


def compute_evolution_by_frequency(
    df,
    id_cols,
    date_col,
    value_col,
    freq=1,
    method='abs',
    format='column',
    offseted_suffix='_offseted',
    evolution_col_name='evolution_computed',
    missing_date_as_zero=False,
    raise_duplicate_error=True
):
    if missing_date_as_zero:
        how = 'outer'
        fillna = 0
    else:
        how = 'left'
        fillna = None

    return __compute_evolution(
        df=df,
        id_cols=id_cols,
        value_col=value_col,
        date_col=date_col,
        freq=freq,
        method=method,
        format=format,
        offseted_suffix=offseted_suffix,
        evolution_col_name=evolution_col_name,
        how=how,
        fillna=fillna,
        raise_duplicate_error=raise_duplicate_error
    )


def compute_evolution_by_criteria(
    df,
    id_cols,
    value_col,
    compare_to,
    method='abs',
    format='column',
    offseted_suffix='_offseted',
    evolution_col_name='evolution_computed',
    raise_duplicate_error=True
):
    return __compute_evolution(**locals())


# old name
compute_evolution = compute_evolution_by_frequency


def __compute_evolution(
    df,
    id_cols,
    value_col,
    date_col=None,
    freq=1,
    compare_to=None,
    method='abs',
    format='column',
    offseted_suffix='_offseted',
    evolution_col_name='evolution_computed',
    how='left',
    fillna=None,
    raise_duplicate_error=True
):
    """
    Compute an evolution column :
        - against a period distant from a fixed frequency.
        - against a part of the df

    Unfortunately, pandas doesn't allow .change() and .pct_change() to be
    executed with a MultiIndex.

    Args:
        df (pd.DataFrame):
        id_cols (list(str)):
        value_col (str):
        date_col (str/dict): default None
        freq (int/pd.DateOffset/pd.Serie): default 1
        compare_to (str): default None
        method (str): default ``'abs'`` can be also ``'pct'``
        format(str): default 'column' can be also 'df'
        offseted_suffix(str): default '_offseted'
        evolution_col_name(str): default 'evolution_computed'
        how(str): default 'left'
        fillna(str/int): default None
    """
    if date_col is not None:
        is_date_to_format = isinstance(date_col, dict) or (df[date_col].dtype == np.object)
        if is_date_to_format:
            if isinstance(date_col, dict):
                date_format = date_col.get('format', None)
                date_col = date_col['selector']
            else:
                date_format = None
            df['_'+date_col + '_copy_'] = pd.to_datetime(df[date_col], format=date_format)
            date_col = '_'+date_col + '_copy_'

        is_freq_dict = isinstance(freq, dict)
        if is_freq_dict:
            freq = pd.DateOffset(**{k: int(v) for k, v in freq.items()})

        check_params_columns_duplicate(id_cols + [value_col, date_col])
        # create df_offseted
        group_cols = id_cols + [date_col]
        df_offseted = df[group_cols + [value_col]].copy()
        df_offseted[date_col] += freq

        df_with_offseted_values = apply_merge(
            df, df_offseted, group_cols, how, offseted_suffix,
            raise_duplicate_error
        )
        if is_date_to_format:
            del df_with_offseted_values[date_col]

    elif compare_to is not None:
        # create df_offseted
        check_params_columns_duplicate(id_cols + [value_col])
        group_cols = id_cols
        df_offseted = df.query(compare_to).copy()
        df_offseted = df_offseted[group_cols + [value_col]]

        df_with_offseted_values = apply_merge(
            df, df_offseted, group_cols, how, offseted_suffix,
            raise_duplicate_error
        )

    apply_fillna(df_with_offseted_values, value_col, offseted_suffix, fillna)
    apply_method(df_with_offseted_values, evolution_col_name, value_col, offseted_suffix, method)
    return apply_format(df_with_offseted_values, evolution_col_name, format)


def apply_merge(df, df_offseted, group_cols, how, offseted_suffix, raise_duplicate_error):
    df_offseted_deduplicated = df_offseted.drop_duplicates(subset=group_cols)

    if df_offseted_deduplicated.shape[0] != df_offseted.shape[0] and how == 'left':
        msg = ("A dataframe for which you want to compute evolutions "
               "has duplicated values against the id_cols you indicated.")
        if raise_duplicate_error:
            raise DuplicateRowsError(msg)
        else:
            logging.getLogger(__name__).warning(f"Warning: {msg}")

    df_with_offseted_values = pd.merge(
        df,
        df_offseted_deduplicated,
        how=how,
        on=group_cols,
        suffixes=['', offseted_suffix]
    ).reset_index(drop=True)

    return df_with_offseted_values


def apply_fillna(df, value_col, offseted_suffix, fillna):
    if fillna is not None:
        df[[value_col, value_col + offseted_suffix]] = \
            df[[value_col, value_col + offseted_suffix]].fillna(fillna)


def apply_method(df, evolution_col, value_col, offseted_suffix, method):
    if method == 'abs':
        df[evolution_col] = (df[value_col] - df[value_col + offseted_suffix])
    elif method == 'pct':
        df_value_as_float = df[value_col + offseted_suffix].astype(float)
        df[evolution_col] = (
            (df[value_col].astype(float) - df_value_as_float) / df_value_as_float
        )
    else:
        raise ValueError("method has to be either 'abs' or 'pct'")


def apply_format(df, evolution_col, format):
    if format == 'df':
        return df
    else:
        return df[evolution_col]


class DuplicateRowsError(Exception):
    """Raised if a dataframe has duplicate rows"""
