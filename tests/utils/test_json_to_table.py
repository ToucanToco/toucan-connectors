import pandas as pd
import pytest

from toucan_connectors.utils.json_to_table import json_to_table

data = pd.DataFrame(
    [
        {
            "name": "blah",
            "list_col": [{"a": 1}, {"a": 2}],
            "adict_col": {"a": 1, "b": {"c": 1}},
            "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
        }
    ]
)


def test_no_op():
    """Should retrun input df"""
    assert json_to_table(data, "name").equals(data)


def test_empty_df():
    empty_df = pd.DataFrame({"name": [], "x": []})
    assert json_to_table(empty_df, "name").equals(empty_df)


def test_multiple_cols_incl_no_op():
    """Should process relevant columns even if one is not json"""
    assert json_to_table(data, ["name", "adict_col"]).equals(
        pd.DataFrame(
            [
                {
                    "name": "blah",
                    "adict_col.a": 1,
                    "adict_col.b.c": 1,
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                }
            ]
        )
    )


def test_list_col():
    """Should return a line per element of the array"""
    assert json_to_table(data, "list_col").equals(
        pd.DataFrame(
            [
                {
                    "list_col.a": 1,
                    "name": "blah",
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                },
                {
                    "list_col.a": 2,
                    "name": "blah",
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                },
            ]
        )
    )


def test_dict_col():
    """Should return one column per key of the array"""
    assert json_to_table(data, "adict_col").equals(
        pd.DataFrame(
            [
                {
                    "name": "blah",
                    "adict_col.a": 1,
                    "adict_col.b.c": 1,
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                }
            ]
        )
    )


def test_dict_list_col():
    """Should return one column per key and one line per elements of the array"""
    assert json_to_table(data, "bdict_list_col").equals(
        pd.DataFrame(
            [
                {
                    "bdict_list_col.a.b": 1,
                    "name": "blah",
                    "bdict_list_col.a": [{"b": 1}, {"b": 2}],
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                },
                {
                    "bdict_list_col.a.b": 2,
                    "name": "blah",
                    "bdict_list_col.a": [{"b": 1}, {"b": 2}],
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                },
            ]
        )
    )


def test_multiple_columns():
    """Should work with an array of columns"""
    assert json_to_table(data, ["list_col", "adict_col"]).equals(
        pd.DataFrame(
            [
                {
                    "name": "blah",
                    "adict_col.a": 1,
                    "adict_col.b.c": 1,
                    "list_col.a": 1,
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                },
                {
                    "name": "blah",
                    "adict_col.a": 1,
                    "adict_col.b.c": 1,
                    "list_col.a": 2,
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                },
            ]
        )
    )


def test_sep():
    """Should work with a custom separator"""
    assert json_to_table(data, "adict_col", sep="_").equals(
        pd.DataFrame(
            [
                {
                    "name": "blah",
                    "adict_col_a": 1,
                    "adict_col_b_c": 1,
                    "list_col": [{"a": 1}, {"a": 2}],
                    "adict_col": {"a": 1, "b": {"c": 1}},
                    "bdict_list_col": {"a": [{"b": 1}, {"b": 2}]},
                }
            ]
        )
    )


def test_value_error():
    """Should raise a value error when called with no columns to merge on"""
    with pytest.raises(ValueError):
        json_to_table(data[["adict_col", "list_col"]], columns="adict_col")
