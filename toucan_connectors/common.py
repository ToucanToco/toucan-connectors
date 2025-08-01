import ast
import asyncio
import dataclasses
import datetime
import logging
import re
from collections.abc import Callable
from contextlib import suppress
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, Undefined, UndefinedError, meta
from jinja2.nativetypes import NativeEnvironment
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import Field

from toucan_connectors.utils.slugify import slugify

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    import sqlalchemy as sa


class NativeImmutableSandboxedEnvironment(NativeEnvironment, ImmutableSandboxedEnvironment): ...


# Query interpolation

RE_PARAM = r"%\(([^(%\()]*)\)s"
RE_JINJA = r"{{([^({{)}]*)}}"
RE_SINGLE_VAR_JINJA = r"{{\s*([^\W\d]\w*)\s*}}"  # a single identifier, e.g: {{ __foo__ }}

RE_JINJA_ALONE = r"^" + RE_JINJA + "$"

# Identify jinja params with no quotes around or complex condition
RE_JINJA_ALONE_IN_STRING = [RE_JINJA + r"([ )])", RE_JINJA + r"()$"]

RE_SET_KEEP_TYPE = r"{{__keep_type__\1}}\2"
RE_GET_KEEP_TYPE = r"{{(__keep_type__[^({{)}]*)}}"
RE_NAMED_PARAM = r"\'?%\([a-zA-Z0-9_]*\)s\'?"


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
    if s.startswith("{{") and s.endswith("}}"):
        inside = s[2:-2]
        return "{{" not in inside and "}}" not in inside
    elif s.startswith("{%") and s.endswith("%}"):
        return True
    else:
        return False


def _has_parameters(query: str) -> bool:
    t = ImmutableSandboxedEnvironment().parse(query)  # noqa: S701
    return bool(meta.find_undeclared_variables(t) or re.search(RE_PARAM, query))


def _prepare_parameters(p: dict | list[dict] | tuple | str) -> dict | list[Any] | tuple | str:
    if isinstance(p, str):
        return repr(p)
    elif isinstance(p, list):
        return [_prepare_parameters(e) for e in p]
    elif isinstance(p, dict):
        return {k: _prepare_parameters(v) for k, v in p.items()}
    else:
        return p


def _prepare_result(res: dict | list[dict] | tuple | str) -> dict | list[Any] | tuple | str:
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
    for elem, rendered_elem in zip(origin, rendered, strict=False):
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
    raise UndefinedVariableError(var_name=var_name, message=f"Undefined variable: {var_name}")


def _is_defined(value: Any) -> bool:
    return not isinstance(value, Undefined)


def _render_query(query: dict | list[dict] | tuple | str, parameters: dict | None, handle_errors: bool = False):
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
        query = re.sub(RE_PARAM, r"{{ \g<1> }}", query)

        # Add quotes to string parameters to keep type if not complex
        clean_p = deepcopy(parameters)

        if is_jinja_alone(query):
            clean_p = _prepare_parameters(clean_p)  # type:ignore[assignment]
            env: Environment | NativeEnvironment = NativeImmutableSandboxedEnvironment()
        else:
            env = ImmutableSandboxedEnvironment()  # noqa: S701

        try:
            res = env.from_string(query).render(clean_p)
        # This happens if we try to access an attribute of an undefined var, i.e nope['nein']
        except UndefinedError:
            return _raise_or_return_undefined(None, handle_errors)

        if isinstance(res, Undefined):
            return _raise_or_return_undefined(res, handle_errors)
        # NativeEnvironment's render() isn't recursive, so we need to
        # apply recursively the literal_eval by hand for lists and dicts:
        if isinstance(res, list | dict):
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

    def _flatten_dict(p, parent_key=""):
        new_p = {}
        for k, v in deepcopy(p).items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            new_p[new_key] = v
            if isinstance(v, list):
                v = dict(enumerate(v))
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
        query = query.replace(key, slugify(key, separator="_"))
    if len(p_keep_type):
        # Add a version of parameters flatten + with quotes for string
        p_keep_type = _flatten_dict(parameters, parent_key="__keep_type_")
        parameters.update(p_keep_type)

    logging.getLogger(__name__).debug(f"Render query: {query} with parameters {parameters}")
    return ImmutableSandboxedEnvironment().from_string(query).render(parameters)


# jq filtering


def transform_with_jq(data: Any, jq_filter: str) -> list:
    import jq

    data = jq.all(jq_filter, data)

    # jq 'multiple outout': the data is already presented as a list of rows
    multiple_output = len(data) == 1 and isinstance(data[0], list)

    # another valid datastructure:  [{col1:[value, ...], col2:[value, ...]}]
    single_cols_dict = isinstance(data[0], dict) and isinstance(list(data[0].values())[0], list)

    if multiple_output or single_cols_dict:
        return data[0]

    return data


FilterSchemaDescription: str = (
    "You can apply filters to json response if data is nested. As we rely on a "
    "library called jq, we suggest the refer to the dedicated "
    '<a href="https://stedolan.github.io/jq/manual/">documentation</a>'
)

FilterSchema = Field(
    ".",
    description=FilterSchemaDescription,
)

XpathSchema = Field(
    "",
    description="You can define an XPath to parse the XML cdata retrieved."
    "For reference visit: "
    '<a href="https://developer.mozilla.org/en-US/docs/Web/XPath">documentation</a>',
)

UI_HIDDEN: dict[str, Any] = {"ui.hidden": True}


def get_loop():
    """Sets up event loop"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop


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


def convert_jinja_params_to_sqlalchemy_named(query: str) -> str:
    """Converts jinja params to SQLAlchemy named parameters.

    Naively transforms '{{ foo }}' to :foo using regex substitution.

    Note that the resulting query should not be used directly, but wrapped with `sqlalchemy.text`
    """
    return re.sub(RE_SINGLE_VAR_JINJA, r":\g<1>", query)


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
            query_string = query_string.replace(extracted_params[i], f"({','.join(len(o) * ['?'])})")

    flattened_values = []
    for val in ordered_values:
        if isinstance(val, list):
            for v in val:
                flattened_values.append(v)
        else:
            flattened_values.append(val)

    return re.sub(RE_NAMED_PARAM, "?", query_string), flattened_values


def convert_to_numeric_paramstyle(query_string: str, params_values: dict) -> tuple[str, tuple[Any]]:
    """Takes a query in pyformat paramstyle and transforms it in numeric paramstyle
       by replacing placeholders by :n and returning values in right order
    ex :
        ('select * from test where id > %(id_nb)s and price > %(price)s;', {"id_nb":1, "price":10}
    returns:
        ('select * from test where id > :1 and price > :2;', (1, 10))"""
    extracted_params = re.findall(RE_NAMED_PARAM, query_string)
    qparams = [get_param_name(m) for m in extracted_params]
    ordered_values = [params_values.get(p) for p in qparams]

    variable_idx = 1
    for i, o in enumerate(ordered_values):
        if isinstance(o, list):
            # if param type is list replace parameter name in query_string by a list of n indexes where n is
            # the length of the list
            # example:
            # query_string = "SELECT name FROM students WHERE age IN %(allowed_ages)"
            # allowed_ages = [16, 17, 18]
            # transformed query_string = "SELECT name FROM students WHERE age IN (:1,:2,:3)"
            list_size = len(o)
            variable_list = f"({','.join([f':{variable_idx + n}' for n in range(list_size)])})"
            query_string = query_string.replace(extracted_params[i], variable_list)
            variable_idx += list_size
        else:
            query_string = query_string.replace(extracted_params[i], f":{variable_idx}")
            variable_idx += 1

    flattened_values = []
    for val in ordered_values:
        if isinstance(val, list):
            for v in val:
                flattened_values.append(v)
        else:
            flattened_values.append(val)

    # NOTE: we should probably return tuple(flattened_values) here but it could be breaking
    return query_string, flattened_values  # type:ignore[return-value]


def convert_to_printf_templating_style(query_string: str) -> str:
    """
    Replaces '{{ foo }}' by '%(foo)s' in the query.
    Useful for sql-based connectors, which, for security reasons, cannot be rendered
    with jinja.
    """
    return re.sub(RE_SINGLE_VAR_JINJA, r"%(\g<1>)s", query_string)


def adapt_param_type(params):
    """Adapts provided params when a conversion is needed. For example, when
    passing a list parameter it should be converted to tuple in order for
    postgres to correctly interpret them as an array.
    """
    return {k: (tuple(v) if isinstance(v, list) else v) for (k, v) in params.items()}


def extract_table_name(query: str) -> str | None:
    m = re.search(r"from\s*(?P<table>[^\s;]+)\s*(where|order by|group by|limit)?", query, re.I)
    return m.group("table") if m else None


def is_interpolating_table_name(query: str) -> bool:
    table_name = extract_table_name(query)
    return bool(table_name and (table_name.startswith("%(") or table_name.startswith(f":{_SQL_PARAMS_PREFIX}")))


def infer_datetime_dtype(df: "pd.DataFrame") -> None:
    """
    Even if a RDBMS table's column has type `date NOT NULL`,
    we get a `object` dtype in the resulting pandas dataframe.
    This util allows to automatically convert it to `datetime64[ns]`.
    """
    import pandas as pd

    for colname in df:
        if df[colname].dtype == "object":
            # get the first non-null value in the series.
            # if it's a datetime, try to convert the whole serie to datetime dtype.
            s = df[colname]
            idx = s.first_valid_index()
            if idx is not None:
                first_value = s.loc[idx]
                if isinstance(first_value, datetime.datetime | datetime.date):
                    with suppress(Exception):
                        df[colname] = pd.to_datetime(df[colname], errors="coerce")


def rename_duplicate_columns(df: "pd.DataFrame") -> None:
    """
    Check if there are duplicated columns in the dataframe.
    If there are, rename them.
    For example, if we have a dataframe with columns ['foo', 'foo'],
    we will rename them to ['foo_0', 'foo_1'].
    """
    import pandas as pd

    cols = pd.Series(df.columns)
    for dup in df.columns[df.columns.duplicated(keep=False)]:
        cols[df.columns.get_loc(dup)] = [f"{dup}_{d_idx}" for d_idx in range(df.columns.get_loc(dup).sum())]  # type:ignore[union-attr]
    df.columns = cols


def render_user_in_query(query: str, params: dict[str, Any]) -> str:
    return ImmutableSandboxedEnvironment().from_string(query).render({"user": params.get("user", {})})


# Matches {{}} with an unlimited number of characters between the brackets, as few times as
# possible, ignoring whitespace
_JINJA_PARAMS_REGEX = re.compile(r"{{\s*(.*?)\s*}}")


# Matches %() suffixed with s, d, or f, and captures the variable name (as few chars as possibl),
# ignoring trailing whitespace
_PYFORMAT_PARAMS_REGEX = re.compile(r"%\(\s*(.*?)\s*\)([sdf])")

_SQL_PARAMS_PREFIX = "__QUERY_PARAM_"


def pyformat_params_to_jinja(query: str) -> str:
    """Convert %()[sdf] params to {{}}"""
    # subsitute matches with {{ <content of the first capture group> }}
    return _PYFORMAT_PARAMS_REGEX.sub(r"{{ \g<1> }}", query)


def unnest_sql_jinja_parameters(query: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Finds all jinja variables in `query`, evaluates them, and flattens their key.

    This will transform `"SELECT * FROM my_table WHERE attr IN {{user.attributes}}"` along with
    `{"user": {"attributes": [1, 2, 3, 4]}}` into
    `"SELECT * FROM my_table WHERE attr IN {{ __QUERY_PARAM_0__ }}"` and
    `{"__QUERY_PARAM_0__": [1, 2, 3, 4]}`.

    This allows to then convert the parameters into any PEP249 paramstyle.
    """
    # finding all jinja params as re.Match objects
    variable_matches = _JINJA_PARAMS_REGEX.finditer(query)
    substituted_params = {}
    substituted_query = query
    # Iterating over reversed matches, as we rebuild the string on-the-fly using match indices, so
    # we need to destroy the end first in order not to impact other matches
    for param_idx, match_ in reversed(list(enumerate(variable_matches))):
        param_name = f"{_SQL_PARAMS_PREFIX}{param_idx}__"
        param_expr = match_.group()
        # evalutating the jinja expr to get the param value
        param_value = nosql_apply_parameters_to_query(param_expr, params)
        substituted_params[param_name] = param_value
        # replacing the previous expr with the new param name
        new_param_repr = "{{ " + param_name + " }}"
        substituted_query = substituted_query[: match_.start()] + new_param_repr + substituted_query[match_.end() :]

    return substituted_query, substituted_params


def pandas_read_sql(
    query: str,
    con,
    params=None,
    adapt_params: bool = False,
    convert_to_qmark: bool = False,
    convert_to_printf: bool = True,
    convert_to_numeric: bool = False,
    render_user: bool = False,
    **kwargs,
) -> "pd.DataFrame":
    import pandas as pd

    if convert_to_printf:
        query = convert_to_printf_templating_style(query)
    if render_user:
        query = ImmutableSandboxedEnvironment().from_string(query).render({"user": params.get("user", {})})
    if convert_to_qmark:
        query, params = convert_to_qmark_paramstyle(query, params)
    if convert_to_numeric:
        query, params = convert_to_numeric_paramstyle(query, params)
    if adapt_params:
        params = adapt_param_type(params)

    try:
        # FIXME: We should use here the sqlalchemy.text() module to
        # escape characters like % but as a quick fix,
        # we added regex replace that will exclude %(.*) compositions
        query = query.replace("%%", "%")
        query = re.sub(r"%[^(%]", r"%\g<0>", query)
        df = pd.read_sql(query, con=con, params=params, **kwargs)
    except pd.errors.DatabaseError as exc:
        if is_interpolating_table_name(query):
            errmsg = f"Execution failed on sql '{query}': interpolating table name is forbidden"
            raise pd.errors.DatabaseError(errmsg) from exc
        else:
            raise

    rename_duplicate_columns(df)
    infer_datetime_dtype(df)
    return df


def create_sqlalchemy_engine(url: "sa.URL", connect_args: dict[str, Any] | None = None) -> "sa.Engine":
    """Creates an SQLAlchemy engine for the given URL.

    Sets sensible connector-specific defaults, such as disabling connection pooling.
    """
    import sqlalchemy as sa

    kwargs: dict[str, Any] = {"poolclass": sa.NullPool}
    if connect_args is not None:
        kwargs["connect_args"] = connect_args

    return sa.create_engine(url, **kwargs)


def pandas_read_sqlalchemy_query(
    *, query: str, engine: "sa.Engine", params: dict[str, Any] | tuple[Any] | None = None
) -> "pd.DataFrame":
    import pandas as pd
    from sqlalchemy import text as sa_text
    from sqlalchemy.exc import SQLAlchemyError

    sa_query = sa_text(query)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(sa_query, conn, params=params)
    except (pd.errors.DatabaseError, SQLAlchemyError) as exc:
        if is_interpolating_table_name(query):
            errmsg = f"Execution failed on sql '{query}': interpolating table name is forbidden"
            raise pd.errors.DatabaseError(errmsg) from exc
        else:
            raise

    rename_duplicate_columns(df)
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

    all_query_params = re.findall(r"{{.*?}}", query)
    for i, query_param in enumerate(all_query_params):
        params[f"__QUERY_PARAM_{i}__"] = nosql_apply_parameters_to_query(query_param, params)
        query = query.replace(query_param, transformer(f"__QUERY_PARAM_{i}__"))

    return query, params
