import numpy as np

__all__ = (
    'lower',
    'upper',
    'title',
    'capitalize',
    'swapcase',
    'length',
    'isalnum',
    'isalpha',
    'isdigit',
    'isspace',
    'islower',
    'isupper',
    'istitle',
    'isnumeric',
    'isdecimal',
    'strip',
    'lstrip',
    'rstrip',
    'center',
    'ljust',
    'rjust',
    'split',
    'rsplit',
    'partition',
    'rpartition',
    'find',
    'rfind',
    'index',
    'rindex',
    'startswith',
    'endswith',
    'concat',
    'contains',
    'repeat',
    'replace_pattern',
    # 'slice',
    # 'slice_replace',
    # 'count'
)


###################################################################################################
#                              METHODS WITH NO EXTRA PARAMETERS
#
# All these functions have the same signature:
# :param df: the dataframe
# :param column: the column
# :param new_column: the destination column (if not set, `column` will be used)
# :return: the transformed dataframe
###################################################################################################

def _generate_basic_str_postprocess(method_name, docstring):
    def f(df, column, new_column=None):
        method = getattr(df[column].str, method_name)
        new_column = new_column or column
        df.loc[:, new_column] = method()
        return df

    f.__name__ = method_name
    f.__doc__ = f'{docstring}\n' \
                f':param df: the dataframe\n' \
                f':param column: the column\n' \
                f':param new_column: the destination column (if not set, `column` will be used)\n' \
                f':return: the transformed dataframe'
    return f


doc = 'Compute length of each string of `column`'
length = _generate_basic_str_postprocess('len', doc)

# lower, upper, capitalize, title, swapcase
###################################################################################################
doc = 'Converts all characters of `column` to lowercase.'
lower = _generate_basic_str_postprocess('lower', doc)

doc = 'Converts all characters of `column` to uppercase.'
upper = _generate_basic_str_postprocess('upper', doc)

doc = 'Converts first character to uppercase and remaining ' \
      'to lowercase for each line of `column`.'
capitalize = _generate_basic_str_postprocess('capitalize', doc)

doc = 'Converts first character to uppercase and remaining ' \
      'to lowercase for each word of each line of `column`.'
title = _generate_basic_str_postprocess('title', doc)

doc = 'Converts uppercase to lowercase and lowercase to uppercase for each word of `column`.'
swapcase = _generate_basic_str_postprocess('swapcase', doc)

# isalnum, isalpha, isdigit, isspace, islower, isupper, istitle, isnumeric, isdecimal
###################################################################################################
doc = 'Check whether all characters in each string in `column` are alphanumeric'
isalnum = _generate_basic_str_postprocess('isalnum', doc)

doc = 'Check whether all characters in each string in `column` are alphabetic'
isalpha = _generate_basic_str_postprocess('isalpha', doc)

doc = 'Check whether all characters in each string in `column` are digits'
isdigit = _generate_basic_str_postprocess('isdigit', doc)

doc = 'Check whether all characters in each string in `column` are whitespace'
isspace = _generate_basic_str_postprocess('isspace', doc)

doc = 'Check whether all characters in each string in `column` are lowercase'
islower = _generate_basic_str_postprocess('islower', doc)

doc = 'Check whether all characters in each string in `column` are uppercase'
isupper = _generate_basic_str_postprocess('isupper', doc)

doc = 'Check whether all characters in each string in `column` are titlecase'
istitle = _generate_basic_str_postprocess('istitle', doc)

doc = 'Check whether all characters in each string in `column` are numeric'
isnumeric = _generate_basic_str_postprocess('isnumeric', doc)

doc = 'Check whether all characters in each string in `column` are decimal'
isdecimal = _generate_basic_str_postprocess('isdecimal', doc)


###################################################################################################
#                                        STRIP METHODS
#
# All these functions have the same signature:
# :param df: the dataframe
# :param column: the column
# :param to_strip: (str: None) set of characters to be removed
# :param new_column: the destination column (if not set, `column` will be used)
# :return: the transformed dataframe
###################################################################################################
def _generate_strip_str_postprocess(method_name, docstring):
    def f(df, column, *, to_strip=None, new_column=None):
        method = getattr(df[column].str, method_name)
        new_column = new_column or column
        df.loc[:, new_column] = method(to_strip)
        return df

    f.__name__ = method_name
    f.__doc__ = f'{docstring}\n' \
                f':param df: the dataframe\n' \
                f':param column: the column\n' \
                f':param to_strip: (str: None) set of characters to be removed\n' \
                f':param new_column: the destination column (if not set, `column` will be used)\n' \
                f':return: the transformed dataframe'
    return f


doc = 'Strip whitespace (including newlines) from each string in `column` from both sides'
strip = _generate_strip_str_postprocess('strip', doc)

doc = 'Strip whitespace (including newlines) from each string in `column` from left side'
lstrip = _generate_strip_str_postprocess('lstrip', doc)

doc = 'Strip whitespace (including newlines) from each string in `column` from left side'
rstrip = _generate_strip_str_postprocess('rstrip', doc)


###################################################################################################
#                              METHODS with `width` and `fillchar`
#
# All these functions have the same signature:
# :param df: the dataframe
# :param column: the column
# :param width: (int) minimum width
# :param fillchar: (default: \' \') additional character for filling
# :param new_column: the destination column (if not set, `column` will be used)
# :return: the transformed dataframe
###################################################################################################

def _generate_width_str_postprocess(method_name, docstring):
    def f(df, column, *, width, fillchar=' ', new_column=None):
        method = getattr(df[column].str, method_name)
        new_column = new_column or column
        df.loc[:, new_column] = method(width, fillchar=fillchar)
        return df

    f.__name__ = method_name
    f.__doc__ = f'{docstring}\n' \
                f':param df: the dataframe\n' \
                f':param column: the column\n' \
                f':param width (int): minimum width\n' \
                f':param fillchar: (default: \' \') additional character for filling\n' \
                f':param new_column: the destination column (if not set, `column` will be used)\n' \
                f':return: the transformed dataframe'
    return f


doc = 'Filling left and right side of strings in `column` with an additional character'
center = _generate_width_str_postprocess('center', doc)

doc = 'Filling right side of strings in `column` with an additional character'
ljust = _generate_width_str_postprocess('ljust', doc)

doc = 'Filling left side of strings in `column` with an additional character'
rjust = _generate_width_str_postprocess('rjust', doc)


###################################################################################################
#                                        SPLIT METHODS
#
# All these functions have the same signature:
# :param df: the dataframe
# :param column: the column
# :param new_columns: the destination columns
#        (if not set, columns `column_1`, ..., `column_n` will be created)
# :param sep: (default: \' \') string or regular expression to split on
# :param limit: (default: None) limit number of splits in output
# :return: the transformed dataframe
###################################################################################################
def _generate_split_str_postprocess(method_name, docstring):
    def f(df, column, *, new_columns=None, sep=' ', limit=None):
        method = getattr(df[column].str, method_name)
        df_split = method(pat=sep, n=limit, expand=True)
        nb_cols = df_split.shape[1]
        if new_columns and (not isinstance(new_columns, list) or nb_cols > len(new_columns)):
            raise ValueError(f"'new_columns' should be a list with at least {nb_cols} elements")
        if new_columns is None:
            new_columns = [f'{column}_{i}' for i in range(1, nb_cols + 1)]
        df[new_columns[:nb_cols]] = df_split
        return df

    f.__name__ = method_name
    f.__doc__ = f'{docstring}\n' \
                f':param df: the dataframe\n' \
                f':param column: the column\n' \
                f':param new_columns: the destination colums\n' \
                f'       (if not set, columns `column_1`, ..., `column_n` will be created)\n' \
                f':param sep: (default: \' \') string or regular expression to split on\n' \
                f':param limit: (default: None) limit number of splits in output\n' \
                f':return: the transformed dataframe'
    return f


doc = 'Split each string in the caller’s values by given pattern, propagating NaN values'
split = _generate_split_str_postprocess('split', doc)

doc = 'Split each string `column` by the given delimiter string, ' \
      'starting at the end of the string and working to the front'
rsplit = _generate_split_str_postprocess('rsplit', doc)


###################################################################################################
#                                        PARTITION METHODS
#
# All these functions have the same signature:
# :param df: the dataframe
# :param column: the column
# :param new_column: the destination column (if not set, `column` will be used)
# :param sep: (default: \' \') string or regular expression to split on
# :return: the transformed dataframe
###################################################################################################
def _generate_partition_str_postprocess(method_name, docstring):
    def f(df, column, *, new_columns, sep=' '):
        if len(new_columns) != 3:
            raise ValueError('`new_columns` must have 3 columns exactly')
        method = getattr(df[column].str, method_name)
        df[new_columns] = method(sep)
        return df

    f.__name__ = method_name
    f.__doc__ = f'{docstring}\n' \
                f':param df: the dataframe\n' \
                f':param column: the column\n' \
                f':param new_column: the destination column (if not set, `column` will be used)\n' \
                f':param sep: (default: \' \') string or regular expression to split on\n' \
                f':return: the transformed dataframe'
    return f


doc = 'Split the string at the first occurrence of sep, and return 3 elements containing ' \
      'the part before the separator, the separator itself, and the part after the separator. ' \
      'If the separator is not found, return 3 elements containing the string itself, ' \
      'followed by two empty strings.'
partition = _generate_partition_str_postprocess('partition', doc)

doc = 'Split the string at the last occurrence of sep, and return 3 elements containing ' \
      'the part before the separator, the separator itself, and the part after the separator. ' \
      'If the separator is not found, return 3 elements containing two empty strings, ' \
      'followed by the string itself.'
rpartition = _generate_partition_str_postprocess('rpartition', doc)


###################################################################################################
#                                   INDEX AND FIND METHODS
#
# All these functions have the same signature:
# :param df: the dataframe
# :param column: the column
# :param new_column: the destination column (if not set, `column` will be used)
# :param sub: substring being searched
# :param start: (default: 0) left edge index
# :param end: (default: None) right edge index
# :return: the transformed dataframe
###################################################################################################
def _generate_find_str_postprocess(method_name, docstring):
    def f(df, column, *, sub, start=0, end=None, new_column=None):
        method = getattr(df[column].str, method_name)
        new_column = new_column or column
        df.loc[:, new_column] = method(sub, start, end)
        return df

    f.__name__ = method_name
    f.__doc__ = f'{docstring}\n' \
                f':param df: the dataframe\n' \
                f':param column: the column\n' \
                f':param new_column: the destination column (if not set, `column` will be used)\n' \
                f':param sub: substring being searched\n' \
                f':param start: (default: 0) left edge index\n' \
                f':param end: (default: None) right edge index\n' \
                f':return: the transformed dataframe'
    return f


doc = 'Return lowest indexes in each strings in `column` where the substring ' \
      'is fully contained between [start:end]. Return -1 on failure'
find = _generate_find_str_postprocess('find', doc)

doc = 'Return highest indexes in each strings in `column` where the substring ' \
      'is fully contained between [start:end]. Return -1 on failure'
rfind = _generate_find_str_postprocess('rfind', doc)

doc = 'Return lowest indexes in each strings where the substring is fully contained ' \
      'between [start:end]. This is the same as str.find except instead of returning -1, ' \
      'it raises a ValueError when the substring is not found'
index = _generate_find_str_postprocess('index', doc)

doc = 'Return highest indexes in each strings where the substring is fully contained ' \
      'between [start:end]. This is the same as str.find except instead of returning -1, ' \
      'it raises a ValueError when the substring is not found'
rindex = _generate_find_str_postprocess('rindex', doc)


###################################################################################################
#                                  STARTSWITH/ENDSWITH METHODS
#
# All these functions have the same signature:
# :param df: the dataframe
# :param column: the column
# :param new_column: the destination column (if not set, `column` will be used)
# :param pat: character sequence
# :param na: (default: NaN) object shown if element tested is not a string
# :return: the transformed dataframe
###################################################################################################
def _generate_with_str_postprocess(method_name, docstring):
    def f(df, column, *, pat, na=np.nan, new_column=None):
        method = getattr(df[column].str, method_name)
        new_column = new_column or column
        df.loc[:, new_column] = method(pat, na=na)
        return df

    f.__name__ = method_name
    f.__doc__ = f'{docstring}\n' \
                f':param df: the dataframe\n' \
                f':param column: the column\n' \
                f':param new_column: the destination column (if not set, `column` will be used)\n' \
                f':param pat: character sequence\n' \
                f':param na: (default: NaN) object shown if element tested is not a string\n' \
                f':return: the transformed dataframe'
    return f


doc = 'Test if the start of each string element matches a pattern.'
startswith = _generate_with_str_postprocess('startswith', doc)

doc = 'Test if the end of each string element matches a pattern.'
endswith = _generate_with_str_postprocess('endswith', doc)


###################################################################################################
#                                        OTHER METHODS
###################################################################################################
def concat(df, *, columns, new_column, sep=None):
    """
    Concatenate `columns` element-wise
    :param df: the dataframe
    :param columns: list of columns to concatenate
    :param new_column: the destination column (if not set, `column` will be used)
    :param sep: (default: None) the separator
    :return: the transformed dataframe
    """
    if len(columns) < 2:
        raise ValueError('The `columns` parameter needs to have at least 2 columns')
    first_col, *other_cols = columns
    df.loc[:, new_column] = df[first_col].astype(str).str.cat(df[other_cols].astype(str), sep=sep)
    return df


def contains(df, column, *, pat, new_column=None, case=True, na=None, regex=True):
    """
    Test if pattern or regex is contained within strings of `column`
    :param df: the dataframe
    :param column: the column
    :param pat: (str) character sequence or regular expression.
    :param new_column: the destination column (if not set, `column` will be used)
    :param case: (bool) if True, case sensitive.
    :param na: fill value for missing values.
    :param regex: (bool) default True
    :return: the transformed dataframe
    """
    new_column = new_column or column
    df.loc[:, new_column] = df[column].str.contains(pat, case=case, na=na, regex=regex)
    return df


def repeat(df, column, *, times, new_column=None):
    """
    Duplicate each string in `column` by indicated number of time
    :param df: the dataframe
    :param column: the column
    :param times: (int) times to repeat the string
    :param new_column: the destination column (if not set, `column` will be used)
    :return: the transformed dataframe
    """
    new_column = new_column or column
    df.loc[:, new_column] = df[column].str.repeat(times)
    return df


def replace_pattern(df, column, *, pat, repl, new_column=None, case=True, regex=True):
    """
    Replace occurrences of pattern/regex in ``column` with some other string
    :param df: the dataframe
    :param column: the column
    :param pat: (str) character sequence or regular expression.
    :param repl: (str) replacement string
    :param new_column: the destination column (if not set, `column` will be used)
    :param case: (bool) if True, case sensitive.
    :param regex: (bool) default True
    :return: the transformed dataframe
    """
    new_column = new_column or column
    df.loc[:, new_column] = df[column].str.replace(pat, repl, case=case, regex=regex)
    return df
