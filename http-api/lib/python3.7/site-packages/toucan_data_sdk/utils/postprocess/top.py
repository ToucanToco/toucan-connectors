def top(df, value, limit, order='asc', group=None):
    """
    Awesome method that achieves what NO query in any language can do: (DRUM ROLL)
    Get the top or flop N results based on a column value for each specified group columns
    Args:
        - group: String or array of strings for the columns,
                 on which you want to perform the group operation
        - value: String for the column name on which you will rank the results
        - order: String 'asc' or 'desc' to sort by ascending ou descending order
        - limit: Number to specify the N results you want to retrieve.
                 Use a positive number x to retrieve the first x results.
                 Use a negative number -x to retrieve the last x results.
    """
    ascending = order != 'desc'
    limit = int(limit)
    filter_func = 'nlargest' if (limit > 0) ^ ascending else 'nsmallest'

    def _top(df):
        return getattr(df, filter_func)(abs(limit), value).sort_values(by=value,
                                                                       ascending=ascending)

    if group is None:
        df = _top(df)
    else:
        df = df.groupby(group).apply(_top)

    return df


def top_group(df, aggregate_by, value, limit, order='asc', function='sum', group=None):
    """
    Get the top or flop N results based on a function and a column value that agregates the input.
    The result is composed by all the original lines including only lines corresponding
    to the top groups
    Args:
        - aggregate_by: Array of strings for the columns you want to aggregate
        - value: String for the column name on which you will aggregate the results
        - order: String 'asc' or 'desc' to sort by ascending ou descending order
        - limit: Number to specify the N results you want to retrieve.
                 Use a positive number x to retrieve the first x results.
                 Use a negative number -x to retrieve the last x results.
        - function (optional): Function to use to group over the group column
        - group(optional): Array of strings for the columns,
                 on which you want to perform the group operation
    """
    aggregate_by = aggregate_by or []
    group_top = group or []
    df2 = df.groupby(group_top + aggregate_by).agg(function).reset_index()
    df2 = top(df2, group=group, value=value, limit=limit, order=order).reset_index(drop=True)
    df2 = df2[group_top + aggregate_by]
    df = df2.merge(df, on=group_top + aggregate_by)

    return df
