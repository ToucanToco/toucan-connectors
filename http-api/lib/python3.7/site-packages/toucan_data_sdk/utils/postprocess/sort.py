def sort(df, columns, order='asc'):
    """
    Sort DataFrame
    :param df: DataFrame
    :param columns: name of the columns to sort
    :param order: asc or desc
    :return: DataFrame
    """
    ascending = order != 'desc'
    return df.sort_values(columns, ascending=ascending)
