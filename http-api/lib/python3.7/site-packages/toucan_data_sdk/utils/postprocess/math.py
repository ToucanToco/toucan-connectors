import operator as _operator
from typing import List

MATH_CHARACTERS = '()+-/*%.'


def _basic_math_operation(df, new_column, column_1, column_2, op):
    """
    Basic mathematical operation to apply operator on `column_1` and `column_2`
    Both can be either a number or the name of a column of `df`
    Will create a new column named `new_column`
    """
    if not isinstance(column_1, (str, int, float)):
        raise TypeError(f'column_1 must be a string, an integer or a float')
    if not isinstance(column_2, (str, int, float)):
        raise TypeError(f'column_2 must be a string, an integer or a float')

    if isinstance(column_1, str):
        column_1 = df[column_1]
    if isinstance(column_2, str):
        column_2 = df[column_2]
    operator = getattr(_operator, op)
    df[new_column] = operator(column_1, column_2)
    return df


def add(df, new_column, column_1, column_2):
    """Add df[value] (value: 'str') or value (number) to column_1"""
    return _basic_math_operation(df, new_column, column_1, column_2, op='add')


def subtract(df, new_column, column_1, column_2):
    """Subtract df[value] (value: 'str') or value (number) to column_1"""
    return _basic_math_operation(df, new_column, column_1, column_2, op='sub')


def multiply(df, new_column, column_1, column_2):
    """Multiply df[value] (value: 'str') or value (number) and column_1"""
    return _basic_math_operation(df, new_column, column_1, column_2, op='mul')


def divide(df, new_column, column_1, column_2):
    """Divide df[value] (value: 'str') or value (number) to column_1"""
    return _basic_math_operation(df, new_column, column_1, column_2, op='truediv')


def is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    else:
        return True


class Token(str):
    """
    A formula is a string like this '"2018  " - 2017 + (a - b)'
    In order to parse it, we split it in different tokens and keep track if it was
    quoted or not.
    E.g. in the formula above, `2017` is a number whereas `"2018"` is a column name.
    even though both are strings.
    """
    def __new__(cls, text, quoted=False):
        string = super().__new__(cls, text.strip())
        string.quoted = quoted
        return string


def _parse_formula(formula_str) -> List[Token]:
    tokens = []
    current_word = ''
    quote_to_match = None
    for x in formula_str:
        if x in ('"', "'") and not quote_to_match:
            quote_to_match = x
            continue
        if x == quote_to_match:
            tokens.append(Token(current_word, True))
            current_word = ''
            quote_to_match = None
            continue
        if quote_to_match or x not in MATH_CHARACTERS:
            current_word += x
        else:
            tokens.append(Token(current_word))
            current_word = ''
            tokens.append(Token(x))
    tokens.append(Token(current_word))
    if quote_to_match is not None:
        raise FormulaError('Missing closing quote in formula')
    return [t for t in tokens if t]


def formula(df, *, new_column, formula):
    """Compute math formula for df and put the result in `column`"""
    tokens = _parse_formula(formula)
    expression_splitted = []
    for t in tokens:
        # To use a column name with only digits, it has to be quoted!
        # Otherwise it is considered as a regular number
        if not t.quoted and (t in MATH_CHARACTERS or is_float(t)):
            expression_splitted.append(t)
        elif t in df.columns:
            expression_splitted.append(f'df["{t}"]')
        else:
            raise FormulaError(f'"{t}" is not a valid column name')
    expression = ''.join(expression_splitted)
    df[new_column] = eval(expression)
    return df


class FormulaError(Exception):
    """Raised when a formula is not valid"""


def round_values(df, *, column, decimals, new_column=None):
    """
    Round each value of `column` and put the result in `new_column`
    (if set to None, `column` will be replaced)

    ENTITY  VALUE_1  VALUE_2
       A     -1.512   -1.504
       A      0.432     0.14

    round_values(df, column='VALUE_1', new_column='Pika', decimals=1) returns:

    ENTITY  VALUE_1  VALUE_2  Pika
       A     -1.512   -1.504  -1.5
       A      0.432     0.14   0.4
    """
    new_column = new_column or column
    df[new_column] = df[column].round(decimals)
    return df


def absolute_values(df, *, column, new_column=None):
    """
    Take the absolute value of each value of `column` and put the result in `new_column`
    (if set to None, `column` will be replaced)

    ENTITY  VALUE_1  VALUE_2
       A     -1.512   -1.504
       A      0.432     0.14

    compute_abs(df, column='VALUE_1', new_column='Pika') returns:

    ENTITY  VALUE_1  VALUE_2   Pika
       A     -1.512   -1.504  1.512
       A      0.432     0.14  0.432
    """
    new_column = new_column or column
    df[new_column] = abs(df[column])
    return df
