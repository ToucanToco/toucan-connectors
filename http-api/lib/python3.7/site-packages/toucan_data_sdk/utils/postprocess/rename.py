def rename(df, values=None, columns=None, locale=None):
    """
    Replaces data values and column names according to locale
    Args:
        df (pd.DataFrame): DataFrame to transform
        values (dict):
            - key (str): term to be replaced
            - value (dict):
                - key: locale
                - value: term's translation
        columns (dict):
            - key (str): columns name to be replaced
            - value (dict):
                - key: locale
                - value: column name's translation
        locale (str): locale
    """
    if values:
        to_replace = list(values.keys())
        value = [values[term][locale] for term in values]
        df = df.replace(to_replace=to_replace, value=value)
    if columns:
        _keys = list(columns.keys())
        _values = [column[locale] for column in columns.values()]
        columns = dict(list(zip(_keys, _values)))
        df = df.rename(columns=columns)
    return df
