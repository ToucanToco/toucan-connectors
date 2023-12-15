# ruff: noqa: N802
from ast import literal_eval
from functools import wraps
from typing import Callable, TypeAlias

from toucan_connectors.condition_translator import ConditionTranslator, Number

FnTakingNumber: TypeAlias = Callable[[type["PandasConditionTranslator"], str, Number], str]


def requires_number(fn: Callable[[type, str, Number | str], str]) -> FnTakingNumber:
    @wraps(fn)
    def wrapper(cls, column, value) -> str:
        # NOTE: We're only literal-evaling here (unquoting) rather than casting to a float because
        # we might want to apply these to dates
        if isinstance(value, str) and ("'" in value or '"' in value):
            value = literal_eval(value)
        return fn(cls, column, value)

    return wrapper


class PandasConditionTranslator(ConditionTranslator):
    """
    Utility class to convert a condition object into pandas.query format

    This is a default way to apply a data filter in connectors, after data has
    been requested and received.
    """

    @classmethod
    def get_column_ref(cls, column: str) -> str:
        """To refer column names (even with spaces or operators), we surround them in backticks"""
        return f"`{column}`"

    @classmethod
    def get_value_str_ref(cls, value: str) -> str:
        return f"'{value}'"

    @classmethod
    def join_clauses(cls, clauses: list[str], logical_operator: str) -> str:
        return "(" + f" {logical_operator} ".join(clauses) + ")"

    @classmethod
    def EQUAL(cls, column: str, value: str | Number) -> str:
        return f"{column} == {value}"

    @classmethod
    def NOT_EQUAL(cls, column: str, value: str | Number) -> str:
        return f"{column} != {value}"

    @classmethod
    @requires_number
    def LOWER_THAN(cls, column: str, value: Number) -> str:
        return f"{column} < {value}"

    @classmethod
    @requires_number
    def LOWER_THAN_EQUAL(cls, column: str, value: Number) -> str:
        return f"{column} <= {value}"

    @classmethod
    @requires_number
    def GREATER_THAN(cls, column: str, value: Number) -> str:
        return f"{column} > {value}"

    @classmethod
    @requires_number
    def GREATER_THAN_EQUAL(cls, column: str, value: Number) -> str:
        return f"{column} >= {value}"

    # NOTE: Here, we cannot ensure that the parameter has the expected type. Some frontend dev has
    # to be expected
    @classmethod
    def IN(cls, column: str, value: str | Number | list[str | Number]) -> str:
        return f"{column} in {value}"

    @classmethod
    def NOT_IN(cls, column: str, value: str | Number | list[str | Number]) -> str:
        return f"{column} not in {value}"
