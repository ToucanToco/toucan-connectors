import pandas as pd


def melt(df, id, value, dropna=False):
    """
    This function is useful to massage a DataFrame into a format where one or more columns
    are identifier variables (id), while all other columns,
    considered measured variables (value), are “unpivoted” to the row axis,
    leaving just two non-identifier columns, ‘variable’ and ‘value’.
    Args:
        df (pd.DataFrame): DataFrame to transform
        id (list): Column(s) to use as identifier variables
        value (list): Column(s) to unpivot.
        dropna (bool): dropna in added 'value' column
    """
    df = df[(id + value)]
    df = pd.melt(df, id_vars=id, value_vars=value)
    if dropna:
        df = df.dropna(subset=['value'])

    return df
