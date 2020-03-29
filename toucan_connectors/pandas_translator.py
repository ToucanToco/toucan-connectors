from typing import List

from toucan_connectors.condition_translator import ConditionTranslator


class PandasConditionTranslator(ConditionTranslator):
    """
    Utility class to convert a condition object into pandas.query format

    This is a default way to apply a data filter in connectors, after data has
    been requested and received.
    """

    @classmethod
    def get_column_ref(cls, column: str) -> str:
        """To refer column names (even with spaces or operators), we surround them in backticks"""
        return f'`{column}`'

    @classmethod
    def get_value_str_ref(cls, value: str) -> str:
        return f"'{value}'"

    @classmethod
    def join_clauses(cls, clauses: List[str], logical_operator: str) -> str:
        return '(' + f' {logical_operator} '.join(clauses) + ')'

    @classmethod
    def EQUAL(cls, column, value) -> str:
        return f'{column} == {value}'

    @classmethod
    def NOT_EQUAL(cls, column, value) -> str:
        return f'{column} != {value}'

    @classmethod
    def LOWER_THAN(cls, column, value) -> str:
        return f'{column} < {value}'

    @classmethod
    def LOWER_THAN_EQUAL(cls, column, value) -> str:
        return f'{column} <= {value}'

    @classmethod
    def GREATER_THAN(cls, column, value) -> str:
        return f'{column} > {value}'

    @classmethod
    def GREATER_THAN_EQUAL(cls, column, value) -> str:
        return f'{column} >= {value}'

    @classmethod
    def IN(cls, column, value) -> str:
        return f'{column} in {value}'

    @classmethod
    def NOT_IN(cls, column, value) -> str:
        return f'{column} not in {value}'
