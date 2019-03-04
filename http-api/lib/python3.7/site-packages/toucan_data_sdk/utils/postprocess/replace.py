def replace(df, column, new_column=None, **kwargs):
    """
    Replace values of a column (uses pandas.Series.replace)
    Args:
        df (pd.DataFrame): DataFrame to transform
        column (str): name of the column containing values to replace
        new_column (str): optional, name of the column which will contain replaced
                          values (same as "column" by default)

        Other parameters are directly forwarded to pandas.Series.replace.
    """
    new_column = new_column or column
    df.loc[:, new_column] = df[column].replace(**kwargs)
    return df
