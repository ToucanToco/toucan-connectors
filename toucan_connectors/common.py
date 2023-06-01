import ast
import asyncio
import dataclasses
import datetime
import logging
import re
from contextlib import suppress
from copy import deepcopy
from typing import Any, Callable

import jq
import pandas as pd
from aiohttp import ClientSession
from jinja2 import Environment, Template, Undefined, UndefinedError, meta
from jinja2.nativetypes import NativeEnvironment
from pydantic import Field
from toucan_data_sdk.utils.helpers import slugify

# Query interpolation

RE_PARAM = r'%\(([^(%\()]*)\)s'
RE_JINJA = r'{{([^({{)}]*)}}'
RE_SINGLE_VAR_JINJA = r'{{\s*([^\W\d]\w*)\s*}}'  # a single identifier, e.g: {{ __foo__ }}

RE_JINJA_ALONE = r'^' + RE_JINJA + '$'

# Identify jinja params with no quotes around or complex condition
RE_JINJA_ALONE_IN_STRING = [RE_JINJA + r'([ )])', RE_JINJA + r'()$']

RE_SET_KEEP_TYPE = r'{{__keep_type__\1}}\2'
RE_GET_KEEP_TYPE = r'{{(__keep_type__[^({{)}]*)}}'
RE_NAMED_PARAM = r'\'?%\([a-zA-Z0-9_]*\)s\'?'


class ClusterStartException(Exception):
    """Raised when start cluster fails"""


def is_jinja_alone(s: str) -> bool:
    """
    Return True if the given string is a jinja template alone.

    For example, these strings are jinja templates alone:
        '{{ foo }}'
        '{{ foo + bar }}'
        '{%if my_indic%}1{%else%}2{%endif%}'

    Whereas these strings are not:
        'Hey {{ foo }} !'
        '{{ foo }}{{ bar }}'

    In the 2nd case, we will always render the result as a string.
    """
    return re.match(RE_JINJA_ALONE, s) or (s.startswith('{%') and s.endswith('%}'))


def _has_parameters(query: dict | list[dict] | tuple | str) -> bool:
    t = Environment().parse(query)
    return bool(meta.find_undeclared_variables(t) or re.search(RE_PARAM, query))


def _prepare_parameters(p: dict | list[dict] | tuple | str) -> dict | list[dict] | tuple | str:
    if isinstance(p, str):
        return repr(p)
    elif isinstance(p, list):
        return [_prepare_parameters(e) for e in p]
    elif isinstance(p, dict):
        return {k: _prepare_parameters(v) for k, v in p.items()}
    else:
        return p


def _prepare_result(res: dict | list[dict] | tuple | str) -> dict | list[dict] | tuple | str:
    if isinstance(res, str):
        return ast.literal_eval(res)
    elif isinstance(res, list):
        return [_prepare_result(e) for e in res]
    elif isinstance(res, dict):
        return {k: _prepare_result(v) for k, v in res.items()}
    else:
        return res


def _flatten_rendered_nested_list(origin: list, rendered: list) -> list:
    """
    Flatten rendered lists in the parent list, so we have the same render logic
    as in toucan frontend's templates.
    """
    result = []
    for elem, rendered_elem in zip(origin, rendered):
        if isinstance(elem, str) and isinstance(rendered_elem, list):
            # a list has been rendered: flatten the result
            result += rendered_elem
        else:
            result.append(rendered_elem)
    return result


class UndefinedVariableError(Exception):
    def __init__(self, message: str | None, var_name: str | None) -> None:
        self.var_name = var_name
        super().__init__(message)


def _raise_or_return_undefined(res: Undefined | None, handle_errors: bool) -> Undefined:
    var_name = None if res is None or res._undefined_name is None else res._undefined_name
    if not handle_errors:
        return res or Undefined(name=var_name)
    # This is publicly documented, so we can safely access it:
    # https://jinja.palletsprojects.com/en/3.0.x/api/#jinja2.Undefined._undefined_name
    raise UndefinedVariableError(var_name=var_name, message=f'Undefined variable: {var_name}')


def _is_defined(value: Any) -> bool:
    return not isinstance(value, Undefined)


def _render_query(
    query: dict | list[dict] | tuple | str, parameters: dict | None, handle_errors: bool = False
):
    """
    Render both jinja or %()s templates in query
    while keeping type of parameters
    """

    if parameters is None:
        return query

    if isinstance(query, dict):
        return {
            key: rendered
            for key, value in deepcopy(query).items()
            if _is_defined(rendered := _render_query(value, parameters, handle_errors))
        }
    elif isinstance(query, list):
        rendered_query = [
            rendered
            for value in deepcopy(query)
            if _is_defined(rendered := _render_query(value, parameters, handle_errors))
        ]
        rendered_query = _flatten_rendered_nested_list(query, rendered_query)
        return rendered_query
    elif isinstance(query, tuple):
        return tuple(
            rendered
            for value in deepcopy(query)
            if _is_defined(rendered := _render_query(value, parameters, handle_errors))
        )
    elif isinstance(query, str):
        if not _has_parameters(query):
            return query

        # Replace param templating with jinja templating:
        query = re.sub(RE_PARAM, r'{{ \g<1> }}', query)

        # Add quotes to string parameters to keep type if not complex
        clean_p = deepcopy(parameters)
        if re.match(RE_JINJA_ALONE, query):
            clean_p = _prepare_parameters(clean_p)

        if is_jinja_alone(query):
            env = NativeEnvironment()
        else:
            env = Environment()

        try:
            res = env.from_string(query).render(clean_p)
        # This happens if we try to access an attribute of an undefined var, i.e nope['nein']
        except UndefinedError:
            return _raise_or_return_undefined(None, handle_errors)

        if isinstance(res, Undefined):
            return _raise_or_return_undefined(res, handle_errors)
        # NativeEnvironment's render() isn't recursive, so we need to
        # apply recursively the literal_eval by hand for lists and dicts:
        if isinstance(res, (list, dict)):
            return _prepare_result(res)
        return res
    else:
        return query


def nosql_apply_parameters_to_query(
    query: dict | list[dict] | tuple | str, parameters: dict | None, handle_errors: bool = False
):
    """
    WARNING : DO NOT USE THIS WITH VARIANTS OF SQL
    Instead use your client library parameter substitution method.
    https://www.owasp.org/index.php/Query_Parameterization_Cheat_Sheet
    """
    rendered = _render_query(query, parameters, handle_errors)
    # If we have undefined, return the default value for the given type
    return rendered if _is_defined(rendered) else type(query)()


def apply_query_parameters(query: str, parameters: dict) -> str:
    """
    Apply parameters to query

    Interpolate the query, which is a Jinja templates, with the provided parameters.
    """

    def _flatten_dict(p, parent_key=''):
        new_p = {}
        for k, v in deepcopy(p).items():
            new_key = f'{parent_key}_{k}' if parent_key else k
            new_p[new_key] = v
            if isinstance(v, list):
                v = {idx: elt for idx, elt in enumerate(v)}
            if isinstance(v, dict):
                new_p.update(_flatten_dict(v, new_key))
            elif isinstance(v, str):
                new_p.update({new_key: f'"{v}"'})
            else:
                new_p.update({new_key: v})
        return new_p

    if parameters is None:
        return query

    # Flag params to keep type if not complex (no quotes or condition)

    for pattern in RE_JINJA_ALONE_IN_STRING:
        query = re.sub(pattern, RE_SET_KEEP_TYPE, query)
    p_keep_type = re.findall(RE_GET_KEEP_TYPE, query)
    for key in p_keep_type:
        query = query.replace(key, slugify(key, separator='_'))
    if len(p_keep_type):
        # Add a version of parameters flatten + with quotes for string
        p_keep_type = _flatten_dict(parameters, parent_key='__keep_type_')
        parameters.update(p_keep_type)

    logging.getLogger(__name__).debug(f'Render query: {query} with parameters {parameters}')
    return Template(query).render(parameters)


# jq filtering


def transform_with_jq(data: object, jq_filter: str) -> list:
    data = jq.all(jq_filter, data)

    # jq 'multiple outout': the data is already presented as a list of rows
    multiple_output = len(data) == 1 and isinstance(data[0], list)

    # another valid datastructure:  [{col1:[value, ...], col2:[value, ...]}]
    single_cols_dict = isinstance(data[0], dict) and isinstance(list(data[0].values())[0], list)

    if multiple_output or single_cols_dict:
        return data[0]

    return data


FilterSchema = Field(
    '.',
    description='You can apply filters to json response if data is nested. As we rely on a '
    'library called jq, we suggest the refer to the dedicated '
    '<a href="https://stedolan.github.io/jq/manual/">documentation</a>',
)

XpathSchema = Field(
    '',
    description='You can define an XPath to parse the XML cdata retrieved.'
    'For reference visit: '
    '<a href="https://developer.mozilla.org/en-US/docs/Web/XPath">documentation</a>',
)


def get_loop():
    """Sets up event loop"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop


async def fetch(url: str, session: ClientSession):
    """Fetch data from an API."""
    async with session.get(url) as res:
        if res.status != 200:
            raise HttpError(
                f'Aborting request due to error from the API: {res.status}, {res.reason}'
            )
        return await res.json()


class HttpError(Exception):
    """
    Raised when the response of an HTTP request has not a 200 status code.
    """


@dataclasses.dataclass()
class ConnectorStatus:
    status: bool | None = None
    message: str | None = None
    error: str | None = None
    details: list[tuple[str, bool | None]] | None = dataclasses.field(default_factory=list)

    def to_dict(self):
        return dataclasses.asdict(self)


def get_param_name(printf_style_argument: str) -> str:
    # %(foobar)s -> foobar
    # '%(foobar)s' -> foobar
    if printf_style_argument.startswith("'"):
        return printf_style_argument[3:-3]
    return printf_style_argument[2:-2]


def convert_to_qmark_paramstyle(query_string: str, params_values: dict) -> tuple[str, list[Any]]:
    """Takes a query in pyformat paramstyle and transforms it in qmark
       by replacing placeholders by ? and returning values in right order
    ex :
        ('select * from test where id > %(id_nb)s and price > %(price)s;', {"id_nb":1, "price":10}
    returns:
        ('select * from test where id > ? and price > ?;', [1, 10])"""

    extracted_params = re.findall(RE_NAMED_PARAM, query_string)
    qparams = [get_param_name(m) for m in extracted_params]
    ordered_values = [params_values.get(p) for p in qparams]

    # Check if we need to replace a list
    for i, o in enumerate(ordered_values):
        if isinstance(o, list):
            # in the query string, replace the ? at index i by the number of item
            # in the provided parameter of type list
            query_string = query_string.replace(
                extracted_params[i], f'({",".join(len(ordered_values[i])*["?"])})'
            )

    flattened_values = []
    for val in ordered_values:
        if isinstance(val, list):
            for v in val:
                flattened_values.append(v)
        else:
            flattened_values.append(val)

    return re.sub(RE_NAMED_PARAM, '?', query_string), flattened_values


def convert_to_printf_templating_style(query_string: str) -> str:
    """
    Replaces '{{ foo }}' by '%(foo)s' in the query.
    Useful for sql-based connectors, which, for security reasons, cannot be rendered
    with jinja.
    """
    return re.sub(RE_SINGLE_VAR_JINJA, r'%(\g<1>)s', query_string)


def adapt_param_type(params):
    """Adapts provided params when a conversion is needed. For example, when
    passing a list parameter it should be converted to tuple in order for
    postgres to correctly interpret them as an array.
    """
    return {k: (tuple(v) if isinstance(v, list) else v) for (k, v) in params.items()}


def extract_table_name(query: str) -> str:
    m = re.search(r'from\s*(?P<table>[^\s;]+)\s*(where|order by|group by|limit)?', query, re.I)
    table = m.group('table')
    return table


def is_interpolating_table_name(query: str) -> bool:
    table_name = extract_table_name(query)
    return table_name.startswith('%(')


def infer_datetime_dtype(df: pd.DataFrame) -> None:
    """
    Even if a RDBMS table's column has type `date NOT NULL`,
    we get a `object` dtype in the resulting pandas dataframe.
    This util allows to automatically convert it to `datetime64[ns]`.
    """
    for colname in df:
        if df[colname].dtype == 'object':
            # get the first non-null value in the series.
            # if it's a datetime, try to convert the whole serie to datetime dtype.
            s = df[colname]
            idx = s.first_valid_index()
            if idx is not None:
                first_value = s.loc[idx]
                if isinstance(first_value, (datetime.datetime, datetime.date)):
                    with suppress(Exception):
                        df[colname] = pd.to_datetime(df[colname], errors='coerce')


def pandas_read_sql(
    query: str,
    con,
    params=None,
    adapt_params: bool = False,
    convert_to_qmark: bool = False,
    convert_to_printf: bool = True,
    render_user: bool = False,
    **kwargs,
) -> pd.DataFrame:
    if convert_to_printf:
        query = convert_to_printf_templating_style(query)
    if render_user:
        query = Template(query).render({'user': params.get('user', {})})
    if convert_to_qmark:
        query, params = convert_to_qmark_paramstyle(query, params)
    if adapt_params:
        params = adapt_param_type(params)

    try:
        # FIXME: We should use here the sqlalchemy.text() module to
        # escape characters like % but as a quick fix,
        # we added regex replace that will exclude %(.*) compositions
        query = query.replace('%%', '%')
        query = re.sub(r'%[^(%]', r'%\g<0>', query)
        df = pd.read_sql(query, con=con, params=params, **kwargs)
    except pd.io.sql.DatabaseError:
        if is_interpolating_table_name(query):
            errmsg = f"Execution failed on sql '{query}': interpolating table name is forbidden"
            raise pd.io.sql.DatabaseError(errmsg)
        else:
            raise

    infer_datetime_dtype(df)
    return df


def sanitize_query(
    query: str, params: dict[str, object], transformer: Callable[[str], str]
) -> tuple[str, dict[str, object]]:
    """
    We allow jinja templates in queries but we don't want to interpolate them directly to avoid SQL injections
    So we extract the jinja templates and replace them with placeholders and interpolate the placeholders separately.
    We then send the query with placeholders and the interpolated values
    to the SQL driver that will reject or not the query!
    """
    import re

    all_query_params = re.findall(r'{{.*?}}', query)
    for i, query_param in enumerate(all_query_params):
        params[f'__QUERY_PARAM_{i}__'] = nosql_apply_parameters_to_query(query_param, params)
        query = query.replace(query_param, transformer(f'__QUERY_PARAM_{i}__'))

    return query, params
