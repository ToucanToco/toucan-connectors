from datetime import date, datetime, timedelta
from typing import Any

import jinja2
import numpy as np
import pandas as pd
import pytest
from jinja2 import Undefined
from pandas.testing import assert_frame_equal
from pytest_mock import MockFixture

from toucan_connectors import common as common_mod
from toucan_connectors.common import (
    ConnectorStatus,
    UndefinedVariableError,
    adapt_param_type,
    apply_query_parameters,
    convert_jinja_params_to_sqlalchemy_named,
    convert_to_numeric_paramstyle,
    convert_to_printf_templating_style,
    convert_to_qmark_paramstyle,
    extract_table_name,
    get_param_name,
    infer_datetime_dtype,
    is_interpolating_table_name,
    nosql_apply_parameters_to_query,
    pandas_read_sql,
    pyformat_params_to_jinja,
    sanitize_query,
)


def test_apply_parameter_to_query_do_nothing():
    """
    It should do nothing
    """
    query = [{"$match": {"domain": "yo", "cat": 1, "step": "2"}}]
    res = nosql_apply_parameters_to_query(query, None)
    assert res == query


@pytest.mark.parametrize(
    "query,params,expected",
    [
        (
            {"$match": {"domain": "truc", "indic": "{{my_indic[0]*my_indic[1]}}"}},
            {"my_indic": [5, 6]},
            {"$match": {"domain": "truc", "indic": 30}},
        ),
        (
            {"$match": {"domain": "truc", "indic": "{%if my_indic%}1{%else%}2{%endif%}"}},
            {"my_indic": False},
            {"$match": {"domain": "truc", "indic": 2}},
        ),
        (
            {
                "$match": {
                    "$and": [
                        {"domain": "truc", "indic0": "{{my_indic[0]}}"},
                        {"indic1": "{{my_indic[1]}}", "indic2": "yo_{{my_indic[2]}}"},
                        {"indic_list": "{{my_indic}}"},
                    ]
                }
            },
            {"my_indic": ["0", 1, "2"]},
            {
                "$match": {
                    "$and": [
                        {"domain": "truc", "indic0": "0"},
                        {"indic1": 1, "indic2": "yo_2"},
                        {"indic_list": ["0", 1, "2"]},
                    ]
                }
            },
        ),
        (
            {
                "$match": {
                    "$and": [
                        {"domain": "truc", "indic0": "%(my_indic_0)s"},
                        {"indic1": "%(my_indic_1)s", "indic2": "yo_%(my_indic_2)s"},
                        {"indic_list": "%(my_indic)s"},
                    ]
                }
            },
            {"my_indic_0": "0", "my_indic_1": 1, "my_indic_2": "2", "my_indic": ["0", 1, "2"]},
            {
                "$match": {
                    "$and": [
                        {"domain": "truc", "indic0": "0"},
                        {"indic1": 1, "indic2": "yo_2"},
                        {"indic_list": ["0", 1, "2"]},
                    ]
                }
            },
        ),
        (
            {
                "$match": {
                    "$and": [
                        {"domain": "truc", "indic0": '{{my_indic["zero"]}}'},
                        {"indic1": '{{my_indic["one"]}}', "indic2": 'yo_{{my_indic["two"]}}'},
                        {"indic_list": "{{my_indic}}"},
                    ]
                }
            },
            {"my_indic": {"zero": "0", "one": 1, "two": "2"}},
            {
                "$match": {
                    "$and": [
                        {"domain": "truc", "indic0": "0"},
                        {"indic1": 1, "indic2": "yo_2"},
                        {"indic_list": {"zero": "0", "one": 1, "two": "2"}},
                    ]
                }
            },
        ),
        (
            {"data": "%(fakirQuery)s"},
            {"fakirQuery": '[{"values":["bibou"],"chartParam":"test","type":"test","name":"test"}]'},
            {"data": '[{"values":["bibou"],"chartParam":"test","type":"test","name":"test"}]'},
        ),
        ({"data": 1}, {}, {"data": 1}),
        ({"data": "1"}, {}, {"data": "1"}),
        (
            {"domain": "blah", "country": {"$ne": "%(country)s"}, "city": "%(city)s"},
            {"city": "Paris"},
            {"domain": "blah", "country": {}, "city": "Paris"},
        ),
        (
            [{"$match": {"country": "%(country)s", "city": "Test"}}, {"$match": {"b": 1}}],
            {"city": "Paris"},
            [{"$match": {"city": "Test"}}, {"$match": {"b": 1}}],
        ),
        (
            {"code": "%(city)s_%(country)s", "domain": "Test"},
            {"city": "Paris"},
            {"code": "Paris_", "domain": "Test"},
        ),
        (
            {"code": "%(city)s_%(country)s", "domain": "Test"},
            {"city": "Paris", "country": "France"},
            {"code": "Paris_France", "domain": "Test"},
        ),
        (
            {"domain": "blah", "country": {"$ne": "{{country}}"}, "city": "{{city}}"},
            {"city": "Paris"},
            {"domain": "blah", "country": {}, "city": "Paris"},
        ),
        (
            {"domain": "blah", "country": {"$eq": "{{country}}"}, "city": "{{city}}"},
            {"city": "Paris", "country": "__VOID__"},
            {"domain": "blah", "country": {"$eq": "__VOID__"}, "city": "Paris"},
        ),
        (
            {"domain": "blah", "country": {"$eq": "__VOID__"}, "city": "{{city}}"},
            {"city": "Paris"},
            {"domain": "blah", "country": {"$eq": "__VOID__"}, "city": "Paris"},
        ),
        (
            {"code": "{{city}}_{{country}}", "domain": "Test"},
            {"city": "Paris", "country": "France"},
            {"code": "Paris_France", "domain": "Test"},
        ),
        (
            {"code": "{{city}}_{{country}}", "domain": "Test"},
            None,
            {"code": "{{city}}_{{country}}", "domain": "Test"},
        ),
        (
            {"column": "date", "operator": "eq", "value": "{{ t + delta }}"},
            {"t": datetime(2020, 12, 31), "delta": timedelta(days=1)},
            {"column": "date", "operator": "eq", "value": datetime(2021, 1, 1)},
        ),
        (
            {"column": "date", "operator": "eq", "value": '{{ t.strftime("%d/%m/%Y") }}'},
            {"t": datetime(2020, 12, 31)},
            {"column": "date", "operator": "eq", "value": "31/12/2020"},
        ),
        (
            {"column": "date", "operator": "in", "value": "{{ allowed_dates }}"},
            {"allowed_dates": [datetime(2020, 12, 31), datetime(2021, 1, 1)]},
            {
                "column": "date",
                "operator": "in",
                "value": [datetime(2020, 12, 31), datetime(2021, 1, 1)],
            },
        ),
        # _flatten_rendered_nested_list tests below:
        (
            {"array": ["{{ entity_id }}", "{{ entity_array }}"]},
            {"entity_id": "1", "entity_array": ["2"]},
            {"array": ["1", "2"]},
        ),
        (
            {"deep": ["{{ entity_array }}", ["4", "5"]]},
            {"entity_array": ["2", "3"]},
            {"deep": ["2", "3", ["4", "5"]]},
        ),
        (
            {"mixed": ["{{ entity_id }}", "{{ entity_array }}"]},
            {"entity_id": 1, "entity_array": [True]},
            {"mixed": [1, True]},
        ),
        # tuple should be rendered tests below:
        (
            {"array": ("{{ one }}", "{{ two }}")},
            {"one": "1", "two": "2"},
            {"array": ("1", "2")},
        ),
        # 'data' should remain a string in these cases:
        (
            {"data": '["{{ my_var }}", "bar"]'},
            {"my_var": "foo"},
            {"data": '["foo", "bar"]'},
        ),
        (
            {"data": '{"x": "{{ my_var }}", "y": "42"}'},
            {"my_var": "foo"},
            {"data": '{"x": "foo", "y": "42"}'},
        ),
        # tests with {% ... %}
        (
            {"data": "{%if count %}{{ count }}{%else%}No{%endif%} chair{% if count != 1 %}s{% endif %}"},
            {"count": 0},
            {"data": "No chairs"},
        ),
        (
            {"data": "{%if count %}{{ count }}{%else%}No{%endif%} chair{% if count != 1 %}s{% endif %}"},
            {"count": 1},
            {"data": "1 chair"},
        ),
        (
            {"data": "{%if obj %}{{ obj }}{%else%}Nothing{%endif%}"},
            {"obj": 0},
            {"data": "Nothing"},
        ),
        (
            {"data": "{%if obj %}{{ obj }}{%else%}Nothing{%endif%}"},
            {"obj": 1},
            {"data": 1},
        ),
        (
            {"data": "{%if obj %}{{ obj }}{%else%}Nothing{%endif%}"},
            {"obj": 1},
            {"data": 1},
        ),
        (
            [{"$match": {"country": '{{country["name"]}}', "city": "Test"}}, {"$match": {"b": 1}}],
            {"city": "Paris"},
            [{"$match": {"city": "Test"}}, {"$match": {"b": 1}}],
        ),
        (
            {"code": "{{city}}_{{country[0]}}", "domain": "Test"},
            {"city": "Paris"},
            {"domain": "Test"},
        ),
        (
            {"answer": "{{ foo + bar }}"},
            {"foo": 40, "bar": 2},
            {"answer": 42},
        ),
        (
            {"value": "{{ (user or dict()).get('attributes', dict()).get('LABEL', 250) }}"},
            {},
            {"value": 250},
        ),
    ],
)
def test_nosql_apply_parameters_to_query(query, params, expected):
    assert nosql_apply_parameters_to_query(query, params) == expected


@pytest.mark.parametrize(
    "query,params,match_",
    [
        (
            [{"$match": {"country": '{{country["name"]}}', "city": "Test"}}, {"$match": {"b": 1}}],
            {"city": "Paris"},
            "country",
        ),
        ({"code": "{{city}}_{{country[0]}}", "domain": "Test"}, {"city": "Paris"}, "country"),
    ],
)
def test_nosql_apply_parameters_to_query_error_on_params(query: dict, params: dict, match_: str):
    with pytest.raises(UndefinedVariableError):
        nosql_apply_parameters_to_query(query, params, handle_errors=True)


def test_nosql_apply_parameters_to_query_unsafe():
    """
    It should prevent any code execution, by using Jinja's sandboxed environement
    """
    with pytest.raises(jinja2.exceptions.SecurityError):
        nosql_apply_parameters_to_query(
            {
                "test": "{% for x in var.__class__.__base__.__subclasses__() %}"
                + "{% if 'warning' in x.__name__ %}"
                + "{{x()._module.__builtins__ ['__import__']"
                + "('os').popen('ls').read()}}"
                + "{% endif %}{% endfor %}"
            },
            {"var": "plop"},
        )
    with pytest.raises(jinja2.exceptions.SecurityError):
        nosql_apply_parameters_to_query({"test": "{{ var.__class__.mro()[-1] }}"}, {"var": "plop"})


def test_nosql_apply_parameters_to_query_dot():
    """It should handle both `x["y"]` and `x.y`"""
    query1 = {"facet": "{{ facet.value }}", "sort": "{{ rank[0] }}", "rows": "{{ bibou[0].value }}"}
    query2 = {
        "facet": '{{ facet["value"] }}',
        "sort": "{{ rank[0] }}",
        "rows": '{{ bibou[0]["value"] }}',
    }
    parameters = {"facet": {"value": "auteur"}, "rank": ["rang"], "bibou": [{"value": 50}]}
    res1 = nosql_apply_parameters_to_query(query1, parameters)
    res2 = nosql_apply_parameters_to_query(query2, parameters)
    assert res1 == res2 == {"facet": "auteur", "sort": "rang", "rows": 50}


def test_render_raw_permission_no_params():
    query = "(indic0 == 0 or indic1 == 1)"
    assert apply_query_parameters(query, None) == query


def test_render_raw_permission():
    query = (
        "(indic0 == {{my_indic[0]}} or indic1 == {{my_indic[1]}}) and "
        'indic2 == "yo_{{my_indic[2]}}" and indic_list == {{my_indic}}'
    )
    params = {"my_indic": ["0", 1, "2"]}
    expected = "(indic0 == \"0\" or indic1 == 1) and indic2 == \"yo_2\" and indic_list == ['0', 1, '2']"
    assert apply_query_parameters(query, params) == expected


def test_bad_variable_in_query():
    """Render empty string if a jinja var is not set"""
    query = {"url": "/stuff/%(thing)s/foo"}
    params = {}
    assert nosql_apply_parameters_to_query(query, params) == {"url": "/stuff//foo"}


def test_connector_status():
    """
    It should be exported as dict
    """
    assert ConnectorStatus(status=True).to_dict() == {
        "status": True,
        "message": None,
        "error": None,
        "details": [],
    }


@pytest.mark.parametrize(
    "query, params, expected_query, expected_ordered_values",
    [
        (
            "select * from test where id > %(id_nb)s and price > %(price)s;",
            {"id_nb": 1, "price": 10},
            "select * from test where id > ? and price > ?;",
            [1, 10],
        ),
        (
            "select * from test where id > %(id_nb)s and id < %(id_nb)s + 1;",
            {"id_nb": 1},
            "select * from test where id > ? and id < ? + 1;",
            [1, 1],
        ),
        (
            "select * from test where id > %(id_nb)s and price > %(price)s;",
            {"id_nb": 1},
            "select * from test where id > ? and price > ?;",
            [1, None],
        ),
        (
            "select * from inventory where quantity in %(quantities)s;",
            {"quantities": [150, 154]},
            "select * from inventory where quantity in (?,?);",
            [150, 154],
        ),
        (
            "select * from test where price > %(__front_var_0__)s;",
            {"__front_var_0__": 1},
            "select * from test where price > ?;",
            [1],
        ),
    ],
)
def test_convert_pyformat_to_qmark(query, params, expected_query, expected_ordered_values):
    """It should return query in qmark paramstyle and values of extracted params"""
    converted_query, ordered_values = convert_to_qmark_paramstyle(query, params)
    assert ordered_values == expected_ordered_values
    assert converted_query == expected_query


def test_convert_to_printf_templating_style():
    """It should convert jinja templates to printf templates only for valid python identifiers"""
    query = "SELECT {{ a }} {{a1}}{{ _a1}} FROM {{ 1a }} {{ aa$%@%}}\n{{aa bb}} hey {{aa_bb }};"
    expected_result = "SELECT %(a)s %(a1)s%(_a1)s FROM {{ 1a }} {{ aa$%@%}}\n{{aa bb}} hey %(aa_bb)s;"
    assert convert_to_printf_templating_style(query) == expected_result


def test_adapt_param_type():
    assert adapt_param_type({"test": [1, 2], "id": 1}) == {"test": (1, 2), "id": 1}


def test_extract_table_name():
    assert extract_table_name("select * from mytable;") == "mytable"
    assert extract_table_name("SELECT * FROM %(plop)s WHERE age > 21;") == "%(plop)s"


def test_is_interpolating_table_name():
    assert is_interpolating_table_name("select * from mytable;") is False
    assert is_interpolating_table_name("SELECT * FROM %(plop)s WHERE age > 21;")


def test_infer_datetime_dtype():
    data = [np.nan, None, datetime(2022, 1, 1, 12, 34, 56), date(2022, 1, 1)]
    df = pd.DataFrame({"date": data}, dtype="object")
    assert df.date.dtype == "object"
    infer_datetime_dtype(df)
    assert df.date.dtype == "datetime64[ns]"
    assert list(df.date.dt.year.dropna()) == [2022.0, 2022.0]


def test_pandas_read_sql_forbidden_interpolation(mocker: MockFixture):
    """
    It should enhance the error provided by pandas' read_sql when someone tries to template a table name
    """
    mocker.patch("pandas.read_sql", side_effect=pd.io.sql.DatabaseError("Some error"))
    with pytest.raises(pd.io.sql.DatabaseError) as e:
        pandas_read_sql(
            query="SELECT * FROM %(tablename)s WHERE Population > 5000000",
            con="sample_connexion",
            params={"tablename": "City"},
        )
    assert "interpolating table name is forbidden" in str(e.value)


def test_pandas_read_sql_error(mocker: MockFixture):
    """
    It should raise the error raised by pandas' read_sql
    """
    mocker.patch("pandas.read_sql", side_effect=pd.io.sql.DatabaseError("Some error"))
    with pytest.raises(pd.io.sql.DatabaseError) as e:
        pandas_read_sql(
            query="SELECT * FROM CITY WHERE Population > %(max_pop)s",
            con="sample_connexion",
            params={"max_pop": 1_000_000},
        )
    assert "Some error" in str(e.value)


def test_pandas_read_sql_duplicate_columns(mocker: MockFixture):
    duplicate_cols_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Paris", "London", "Berlin"],
            "id2": [4, 5, 6],
        }
    )
    duplicate_cols_df.rename(columns={"id2": "id"}, inplace=True)
    mocker.patch("pandas.read_sql", return_value=duplicate_cols_df)
    df = pandas_read_sql(query="SELECT * FROM CITY", con="sample_connection")
    assert_frame_equal(
        df,
        pd.DataFrame(
            {
                "id_0": [1, 2, 3],
                "name": ["Paris", "London", "Berlin"],
                "id_1": [4, 5, 6],
            }
        ),
    )


def test_get_param_name():
    assert get_param_name("'%(FOOBAR)s'") == "FOOBAR"
    assert get_param_name("%(FOOBAR)s") == "FOOBAR"


def test_convert_to_qmark():
    assert convert_to_qmark_paramstyle("SELECT * FROM foobar WHERE x = %(value)s", {"value": 42}) == (
        "SELECT * FROM foobar WHERE x = ?",
        [42],
    )
    assert convert_to_qmark_paramstyle("SELECT * FROM foobar WHERE x = '%(value)s'", {"value": 42}) == (
        "SELECT * FROM foobar WHERE x = ?",
        [42],
    )


@pytest.mark.parametrize(
    "query,init_params,transformer,expected_query,expected_params",
    [
        (
            "SELECT * FROM foobar WHERE x = {{ value }}",
            {"value": 42},
            lambda x: f"%({x})s",
            "SELECT * FROM foobar WHERE x = %(__QUERY_PARAM_0__)s",
            {"value": 42, "__QUERY_PARAM_0__": 42},
        ),
        (
            "SELECT * FROM foobar WHERE x = {{ value }}",
            {"value": 42},
            lambda x: f":{x}",
            "SELECT * FROM foobar WHERE x = :__QUERY_PARAM_0__",
            {"value": 42, "__QUERY_PARAM_0__": 42},
        ),
        (
            "SELECT * FROM foobar WHERE x = {{ values[0] }}",
            {"values": [17, 42]},
            lambda x: f"%({x})s",
            "SELECT * FROM foobar WHERE x = %(__QUERY_PARAM_0__)s",
            {"values": [17, 42], "__QUERY_PARAM_0__": 17},
        ),
        (
            'SELECT * FROM {{ tables["tableb"] }} WHERE x = {{ values["x"] }} AND y = {{ values["ys"][1] }} ORDER BY id',  # noqa: E501
            {"tables": {"a": "theTableA", "tableb": "theTableB"}, "values": {"x": 1, "ys": [2, 3]}},
            lambda x: f"%({x})s",
            "SELECT * FROM %(__QUERY_PARAM_0__)s WHERE x = %(__QUERY_PARAM_1__)s AND y = %(__QUERY_PARAM_2__)s ORDER BY id",  # noqa: E501
            {
                "tables": {"a": "theTableA", "tableb": "theTableB"},
                "values": {"x": 1, "ys": [2, 3]},
                "__QUERY_PARAM_0__": "theTableB",
                "__QUERY_PARAM_1__": 1,
                "__QUERY_PARAM_2__": 3,
            },
        ),
    ],
)
def test_sanitize_query(query, init_params, transformer, expected_query, expected_params):
    assert sanitize_query(query, init_params, transformer) == (expected_query, expected_params)


@pytest.mark.parametrize(
    "query, expected",
    [("{{nope}}", ""), ({"x": "{{nope}}"}, {}), (("{{nope}}",), ()), ([{"x": "{{nope}}"}], [])],
)
def test_nosql_apply_parameters_to_query_root_undefined(query: Any, expected: Any, mocker: MockFixture):
    mocker.patch.object(common_mod, "_render_query", return_value=Undefined())
    assert nosql_apply_parameters_to_query(query=query, parameters={}, handle_errors=False) == expected


@pytest.mark.parametrize(
    "query, params, expected_query, expected_ordered_values",
    [
        (
            "select * from test where id > %(id_nb)s and price > %(price)s;",
            {"id_nb": 1, "price": 10},
            "select * from test where id > :1 and price > :2;",
            [1, 10],
        ),
        (
            "select * from test where id > %(id_nb)s and id < %(id_nb)s + 1;",
            {"id_nb": 1},
            "select * from test where id > :1 and id < :1 + 1;",
            [1, 1],
        ),
        (
            "select * from test where id > %(id_nb)s and price > %(price)s;",
            {"id_nb": 1},
            "select * from test where id > :1 and price > :2;",
            [1, None],
        ),
        (
            "select * from inventory where quantity in %(quantities)s;",
            {"quantities": [150, 154]},
            "select * from inventory where quantity in (:1,:2);",
            [150, 154],
        ),
        (
            "select * from test where price > %(__front_var_0__)s;",
            {"__front_var_0__": 1},
            "select * from test where price > :1;",
            [1],
        ),
    ],
)
def test_convert_pyformat_to_numeric(query, params, expected_query, expected_ordered_values):
    """It should return query in numeric paramstyle and values of extracted params"""
    converted_query, ordered_values = convert_to_numeric_paramstyle(query, params)
    assert ordered_values == expected_ordered_values
    assert converted_query == expected_query


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT * FROM my_table;", "SELECT * FROM my_table;"),
        (
            "SELECT name, population FROM City WHERE name SIMILAR TO '%aastri%' AND population >= {{min_pop}}",
            "SELECT name, population FROM City WHERE name SIMILAR TO '%aastri%' AND population >= :min_pop",
        ),
    ],
)
def test_convert_jinja_params_to_sqlalchemy_named(query: str, expected: str) -> None:
    result = convert_jinja_params_to_sqlalchemy_named(query)
    assert result == expected


@pytest.mark.parametrize(
    "query,expected_query",
    [
        (
            "SELECT * FROM City WHERE Population > %(max_pop)s",
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
        ),
        (
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
            "SELECT * FROM City WHERE Population > {{ max_pop }}",
        ),
        (
            "SELECT %(max_pop)s, City.* FROM City WHERE Population > %(max_pop)d",
            "SELECT {{ max_pop }}, City.* FROM City WHERE Population > {{ max_pop }}",
        ),
        (
            """SELECT %( manif   )s, {{ user['email']   }}, City.* FROM City WHERE LifeExpectancy > {{user.attributes["age_years"]}}""",  # noqa:E501
            """SELECT {{ manif }}, {{ user['email']   }}, City.* FROM City WHERE LifeExpectancy > {{user.attributes["age_years"]}}""",  # noqa:E501
        ),
        (
            """SELECT %(user.email)s, {{user.attributes["age_years"]}}, {{ user.attributes.fib[2]}} FROM City WHERE LifeExpectancy > {{user.attributes.fib[4] * 10}}""",  # noqa:E501
            """SELECT {{ user.email }}, {{user.attributes["age_years"]}}, {{ user.attributes.fib[2]}} FROM City WHERE LifeExpectancy > {{user.attributes.fib[4] * 10}}""",  # noqa:E501
        ),
    ],
)
def test_pyformat_params_to_jinja(query: str, expected_query: str) -> None:
    assert pyformat_params_to_jinja(query) == expected_query
