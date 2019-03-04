"""date filtering helpers."""

from datetime import date, datetime
import re
from uuid import uuid4

import pandas as pd


def _norm_date(datestr: str, date_fmt: str) -> date:
    """normalize symbolic date values (e.g. 'TODAY')

    Convert a symbolic value in a valid date.
    Currenlty known symbolic values are 'TODAY', 'YESTERDAY' and 'TOMORROW'.

    NOTE: This function will return `date` (not `datetime`) instances.

    Parameters:
        `datestr`: the date to parse, formatted as `date_fmt`
        `date_fmt`: expected output date format

    Returns:
        The interpreted date as a datetime.datetime object.
        If `datestr` doesn't match any of the known symbolic names, it just parses it.

    """
    try:
        days = {'TODAY': 0, 'YESTERDAY': -1, 'TOMORROW': 1}[datestr.upper()]
        return date.today() + pd.Timedelta(days=days)
    except KeyError:
        return datetime.strptime(datestr, date_fmt).date()


def parse_date(datestr: str, date_fmt: str) -> date:
    """parse `datestr` and return corresponding date object.

    `datestr` should be a string matching `date_fmt` and parseable by
    `strptime` but some offset can also be added using `(datestr) + OFFSET` or
    `(datestr) - OFFSET` syntax. When using this syntax, `OFFSET` should be
    understable by `pandas.Timedelta` (cf.
    http://pandas.pydata.org/pandas-docs/stable/timedeltas.html) and `datestr`
    MUST be wrapped with parenthesis.

    Additionally, the following symbolic names are supported:
    `TODAY`, `YESTERDAY`, `TOMORROW`.

    Example usage:

    >>> parse_date('2018-01-01', '%Y-%m-%d')
    datetime.date(2018, 1, 1)
    >>> parse_date('(2018-01-01) + 1day', '%Y-%m-%d')
    datetime.date(2018, 1, 2)

    Parameters:
        `datestr`: the date to parse, formatted as `date_fmt`
        `date_fmt`: expected date format

    Returns:
        The `date` object. If date could not be parsed, a ValueError
        will be raised.
    """
    rgx = re.compile(r'\((?P<date>.*)\)(\s*(?P<sign>[+-])(?P<offset>.*))?$')
    datestr = datestr.strip()
    match = rgx.match(datestr)
    # if regexp doesn't match, date must match the expected format
    if match is None:
        return _norm_date(datestr, date_fmt)
    datestr = match.group('date').strip()
    dateobj = _norm_date(datestr, date_fmt)
    offset = match.group('offset')
    if offset:
        sign = match.group('sign')
        delta = pd.Timedelta(offset)
        dateobj = (dateobj + delta) if sign == '+' else (dateobj - delta)
    return dateobj


def filter_by_date(df, date_col, date_fmt='%Y-%m-%d', start=None, stop=None, atdate=None):
    """filter dataframe `df` by date.

    This function will interpret `start`, `stop` and `atdate` and build
    the corresponding date range. The caller must specify either:

    - `atdate`: keep all rows matching this date exactly,
    - `start`: keep all rows matching this date onwards.
    - `stop`: keep all rows matching dates before this one.
    - `start` and `stop`: keep all rows between `start` and `stop`,

    Any other combination will raise an error. The lower bound of the date range
    will be included, the upper bound will be excluded.

    When specified, `start`, `stop` and `atdate` values are expected to match the
    `date_fmt` format or a known symbolic value (i.e. 'TODAY', 'YESTERDAY' or
    'TOMORROW').

    Additionally, the offset syntax "(date) + offset" is also supported (Mind
    the parenthesis around the date string). In that case, the offset must be
    one of the syntax supported by `pandas.Timedelta` (cf.
    http://pandas.pydata.org/pandas-docs/stable/timedeltas.html)

    Parameters:
        `df`: the dataframe to filter
        `date_col`: the name of the dataframe's column to filter on
        `date_fmt`: expected date format in column `date_col`
        `start`: if specified, lower bound (included) of the date range
        `stop`: if specified, upper bound (excluded) of the date range
        `atdate`: if specified, the exact date we're filtering on

    Returns:
        The filtered dataframe
    """
    mask = None
    if start is None and stop is None and atdate is None:
        raise TypeError('either "start", "stop" or "atdate" must be specified')
    if start is not None and atdate is not None:
        raise TypeError('"start" and "atdate" are mutually exclusive')
    if stop is not None and atdate is not None:
        raise TypeError('"stop" and "atdate" are mutually exclusive')
    # add a new column that will hold actual date objects instead of strings.
    # This column is just temporary and will be removed before returning the
    # filtered dataframe.
    filtercol = str(uuid4())
    df[filtercol] = pd.to_datetime(df[date_col], format=date_fmt)
    if atdate is not None:
        mask = df[filtercol] == parse_date(atdate, date_fmt)
    elif start is not None and stop is not None:
        mask = ((df[filtercol] >= parse_date(start, date_fmt)) &
                (df[filtercol] < parse_date(stop, date_fmt)))
    elif stop is None:
        mask = df[filtercol] >= parse_date(start, date_fmt)
    elif start is None:
        mask = df[filtercol] < parse_date(stop, date_fmt)
    return df[mask].drop(filtercol, axis=1)
