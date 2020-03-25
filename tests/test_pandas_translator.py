import pytest

from toucan_connectors.pandas_translator import PandasConditionTranslator


def test_permission_condition_to_pandas_clause():
    # works with list
    c = {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']}
    assert PandasConditionTranslator.condition_to_clause(c) == "`city name` in ['Paris', 'London']"
    # works with numbers
    c = {'column': 'population', 'operator': 'eq', 'value': 42}
    assert PandasConditionTranslator.condition_to_clause(c) == '`population` == 42'
    # override enclosing field char
    assert PandasConditionTranslator.condition_to_clause(c, "'") == "'population' == 42"
    # put strings between ''
    c = {'column': 'country', 'operator': 'eq', 'value': 'France'}
    assert PandasConditionTranslator.condition_to_clause(c) == "`country` == 'France'"
    # looking for 100% code coverage
    with pytest.raises(KeyError):
        PandasConditionTranslator.condition_to_clause({'column': 'population', 'operator': 'eq'})
    with pytest.raises(KeyError):
        PandasConditionTranslator.condition_to_clause({'column': 'population', 'value': 42})
    with pytest.raises(KeyError):
        PandasConditionTranslator.condition_to_clause({'operator': 'eq', 'value': 42})
    with pytest.raises(ValueError):
        PandasConditionTranslator.condition_to_clause(
            {'column': 'population', 'operator': 'unsupported', 'value': 42}
        )
    with pytest.raises(Exception):
        PandasConditionTranslator.condition_to_clause(
            {'column': 'population', 'operator': 'matches', 'value': 42}
        )


def test_permission_conditions_to_pandas_query():
    c = {
        'and': [
            {'column': 'country', 'operator': 'eq', 'value': 'France'},
            {
                'or': [
                    {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']},
                    {'column': 'population', 'operator': 'eq', 'value': 42},
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
        PandasConditionTranslator.translate({'and': 1})
    with pytest.raises(ValueError):
        PandasConditionTranslator.translate({'or': 1})


def test_PandasConditionTranslator_operators():
    assert PandasConditionTranslator.EQUAL()('col', 'val') == 'col == val'
    assert PandasConditionTranslator.NOT_EQUAL()('col', 'val') == 'col != val'
    assert PandasConditionTranslator.GREATER_THAN()('col', 'val') == 'col > val'
    assert PandasConditionTranslator.GREATER_THAN_EQUAL()('col', 'val') == 'col >= val'
    assert PandasConditionTranslator.LOWER_THAN()('col', 'val') == 'col < val'
    assert PandasConditionTranslator.LOWER_THAN_EQUAL()('col', 'val') == 'col <= val'
    assert PandasConditionTranslator.IN()('col', ['val']) == "col in ['val']"
    assert PandasConditionTranslator.NOT_IN()('col', ['val']) == "col not in ['val']"
