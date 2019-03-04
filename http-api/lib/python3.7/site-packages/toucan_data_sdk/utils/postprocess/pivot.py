import numpy as np
import pandas as pd


def pivot(df, index, column, value):
    """
    Pivot a dataframe. Reverse operation of melting. Useful for configuring evolution
    See pandas' pivot_table documentation for more details
    Args:
        - index (list): indexes argument of pd.pivot_table
        - column (str): column name to pivot on
        - value (str): column name containing the value to fill the pivoted df
    """
    if df.dtypes[value].type == np.object_:
        df = pd.pivot_table(df, index=index,
                            columns=column,
                            values=value,
                            aggfunc=lambda x: ' '.join(x))
    else:
        df = pd.pivot_table(df, index=index,
                            columns=column,
                            values=value)
    df = df.reset_index()
    return df


def pivot_by_group(df, variable, value, new_columns, groups, id_cols=None):
    if id_cols is None:
        index = [variable]
    else:
        index = [variable] + id_cols

    param = pd.DataFrame(groups, index=new_columns)
    temporary_colum = 'tmp'

    df[temporary_colum] = df[variable]
    for column in param.columns:
        df.loc[df[variable].isin(param[column]), variable] = column

    param = param.T
    for column in param.columns:
        df.loc[
            df[temporary_colum].isin(param[column]), temporary_colum] = column

    df = pivot(df, index, temporary_colum, value)
    return df
