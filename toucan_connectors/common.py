import ast
import asyncio
import dataclasses
import re
from copy import deepcopy
from typing import List, Optional, Tuple

import pandas as pd
import pyjq
from aiohttp import ClientSession
from jinja2 import Environment, StrictUndefined, Template, meta
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
RE_NAMED_PARAM = r'%\([a-zA-Z1-9_]*\)s'


class NonValidVariable(Exception):
    """ Error thrown for a non valid variable in endpoint """


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


def nosql_apply_parameters_to_query(query, parameters, handle_errors=False):
    """
    WARNING : DO NOT USE THIS WITH VARIANTS OF SQL
    Instead use your client library parameter substitution method.
    https://www.owasp.org/index.php/Query_Parameterization_Cheat_Sheet
    """

    def _has_parameters(query):
        t = Environment().parse(query)
        return meta.find_undeclared_variables(t) or re.search(RE_PARAM, query)

    def _prepare_parameters(p):
        if isinstance(p, str):
            return repr(p)
        elif isinstance(p, list):
            return [_prepare_parameters(e) for e in p]
        elif isinstance(p, dict):
            return {k: _prepare_parameters(v) for k, v in p.items()}
        else:
            return p

    def _prepare_result(res):
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

    def _render_query(query, parameters):
        """
        Render both jinja or %()s templates in query
        while keeping type of parameters
        """
        if isinstance(query, dict):
            return {key: _render_query(value, parameters) for key, value in deepcopy(query).items()}
        elif isinstance(query, list):
            rendered_query = [_render_query(elt, parameters) for elt in deepcopy(query)]
            rendered_query = _flatten_rendered_nested_list(query, rendered_query)
            return rendered_query
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

            res = env.from_string(query).render(clean_p)
            # NativeEnvironment's render() isn't recursive, so we need to
            # apply recursively the literal_eval by hand for lists and dicts:
            if isinstance(res, (list, dict)):
                return _prepare_result(res)
            return res
        else:
            return query

    def _handle_missing_params(elt, params, handle_errors):
        """
        Remove a dictionary key if its value has a missing parameter.
        This is used to support the __VOID__ syntax, specific at Toucan Toco :
            cf. https://bit.ly/2Ln6rcf
        """
        if isinstance(elt, dict):
            e = {}
            for k, v in elt.items():
                if isinstance(v, str):
                    matches = re.findall(RE_PARAM, v) + re.findall(RE_JINJA, v)
                    missing_params = []
                    for m in matches:
                        try:
                            Template('{{ %s }}' % m, undefined=StrictUndefined).render(params)
                        except Exception:
                            if handle_errors:
                                raise NonValidVariable(f'Non valid variable {m}')
                            missing_params.append(m)
                    if any(missing_params):
                        continue
                    else:
                        e[k] = v
                else:
                    e[k] = _handle_missing_params(v, params, handle_errors)
            return e
        elif isinstance(elt, list):
            return [_handle_missing_params(e, params, handle_errors) for e in elt]
        else:
            return elt

    query = _handle_missing_params(query, parameters, handle_errors)

    if parameters is None:
        return query

    query = _render_query(query, parameters)
    return query


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

    return Template(query).render(parameters)


# jq filtering


def transform_with_jq(data: object, jq_filter: str) -> list:
    data = pyjq.all(jq_filter, data)

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
    status: Optional[bool] = None
    message: Optional[str] = None
    error: Optional[str] = None
    details: Optional[List[Tuple[str, Optional[bool]]]] = dataclasses.field(default_factory=list)

    def to_dict(self):
        return dataclasses.asdict(self)


def convert_to_qmark_paramstyle(query_string: str, params_values: dict) -> str:
    """Takes a query in pyformat paramstyle and transforms it in qmark
       by replacing placeholders by ? and returning values in right order
    ex :
        ('select * from test where id > %(id_nb)s and price > %(price)s;', {"id_nb":1, "price":10}
    returns:
        ('select * from test where id > ? and price > ?;', [1, 10])"""

    extracted_params = re.findall(RE_NAMED_PARAM, query_string)
    qparams = [m[2:-2] for m in extracted_params]
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
        df = pd.read_sql(query, con=con, params=params, **kwargs)
    except pd.io.sql.DatabaseError:
        if is_interpolating_table_name(query):
            errmsg = f"Execution failed on sql '{query}': interpolating table name is forbidden"
            raise pd.io.sql.DatabaseError(errmsg)
        else:
            raise

    return df
