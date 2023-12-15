import pytest

from toucan_connectors.pandas_translator import PandasConditionTranslator


def test_translate_condition_unit():
    # works with list
    c = {"column": "city name", "operator": "in", "value": ["Paris", "London"]}
    assert PandasConditionTranslator.translate(c) == "`city name` in ['Paris', 'London']"
    # works with numbers
    c = {"column": "population", "operator": "eq", "value": 42}
    assert PandasConditionTranslator.translate(c) == "`population` == 42"
    # put strings between ''
    c = {"column": "country", "operator": "eq", "value": "France"}
    assert PandasConditionTranslator.translate(c) == "`country` == 'France'"
    # error cases
    with pytest.raises(ValueError):
        PandasConditionTranslator.translate({"column": "population", "operator": "eq"})
    with pytest.raises(ValueError):
        PandasConditionTranslator.translate({"column": "population", "value": 42})
    with pytest.raises(ValueError):
        PandasConditionTranslator.translate({"operator": "eq", "value": 42})
    with pytest.raises(ValueError):
        PandasConditionTranslator.translate({"column": "population", "operator": "unsupported", "value": 42})
    with pytest.raises(Exception):
        PandasConditionTranslator.translate({"column": "population", "operator": "matches", "value": 42})


def test_translate_condition_to_pandas_query():
    c = {
        "and": [
            {"column": "country", "operator": "eq", "value": "France"},
            {
                "or": [
                    {"column": "city name", "operator": "in", "value": ["Paris", "London"]},
                    {"column": "population", "operator": "eq", "value": 42},
                ]
            },
        ]
    }
    assert (
        PandasConditionTranslator.translate(c)
        == "(`country` == 'France' and (`city name` in ['Paris', 'London'] or `population` == 42))"
    )
    # Invalid and/or condition list
    with pytest.raises(ValueError):
        PandasConditionTranslator.translate({"and": 1})
    with pytest.raises(ValueError):
        PandasConditionTranslator.translate({"or": 1})


def test_PandasConditionTranslator_operators():
    assert PandasConditionTranslator.EQUAL("col", "val") == "col == val"
    assert PandasConditionTranslator.NOT_EQUAL("col", "val") == "col != val"
    assert PandasConditionTranslator.GREATER_THAN("col", "val") == "col > val"
    assert PandasConditionTranslator.GREATER_THAN_EQUAL("col", "val") == "col >= val"
    assert PandasConditionTranslator.LOWER_THAN("col", "val") == "col < val"
    assert PandasConditionTranslator.LOWER_THAN_EQUAL("col", "val") == "col <= val"
    assert PandasConditionTranslator.IN("col", ["val"]) == "col in ['val']"
    assert PandasConditionTranslator.NOT_IN("col", ["val"]) == "col not in ['val']"


def test_PandasConditionTranslator_operators_with_quoted_strings():
    assert PandasConditionTranslator.EQUAL("col", '"val"') == 'col == "val"'
    assert PandasConditionTranslator.NOT_EQUAL("col", "'val'") == "col != 'val'"
    assert PandasConditionTranslator.GREATER_THAN("col", '"42"') == "col > 42"
    assert PandasConditionTranslator.GREATER_THAN_EQUAL("col", "'-42'") == "col >= -42"
    assert PandasConditionTranslator.LOWER_THAN("col", '"""42.1"""') == "col < 42.1"
    assert PandasConditionTranslator.LOWER_THAN_EQUAL("col", "'''-42.1'''") == "col <= -42.1"
    assert PandasConditionTranslator.IN("col", ["val"]) == "col in ['val']"
    assert PandasConditionTranslator.NOT_IN("col", ["val"]) == "col not in ['val']"


def test_PandasConditionTranslator_operators_with_numbers():
    assert PandasConditionTranslator.EQUAL("col", 42) == "col == 42"
    assert PandasConditionTranslator.NOT_EQUAL("col", 42.12) == "col != 42.12"
    assert PandasConditionTranslator.GREATER_THAN("col", -42) == "col > -42"
    assert PandasConditionTranslator.GREATER_THAN_EQUAL("col", -42.12) == "col >= -42.12"
    assert PandasConditionTranslator.LOWER_THAN("col", 42) == "col < 42"
    assert PandasConditionTranslator.LOWER_THAN_EQUAL("col", 42.12) == "col <= 42.12"
    assert PandasConditionTranslator.IN("col", [-42]) == "col in [-42]"
    assert PandasConditionTranslator.NOT_IN("col", [-42.12]) == "col not in [-42.12]"
