def drop_duplicates(df, columns):
    """
    Use only `columns` to identify duplicates and remove them.
    Set `columns` to None to use all of the columns
    """
    return df.drop_duplicates(columns)


def query_df(df, query):
    """
    Slice the data according to the provided query
    Basic usage like the one in the data query but in the postprocess
    Useful if you want to perform some slicing after a melt or pivot for example.
    Wired on the DataFrame.query method, see doc
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.query.html#pandas.DataFrame.query
    Args:
        Query String
    """
    df = df.query(query)
    return df


query = query_df
