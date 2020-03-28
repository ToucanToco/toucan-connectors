from toucan_connectors.common import ConditionTranslator


class PandasConditionTranslator(ConditionTranslator):
    """
    Utility class to convert a condition object into pandas.query format
    """

    @classmethod
    def translate(cls, conditions: dict, enclosing_field_char='`', enclosing_value_char="'") -> str:
        return super().translate(
            conditions,
            enclosing_field_char=enclosing_field_char,
            enclosing_value_char=enclosing_value_char,
        )

    @classmethod
    def join_clauses(cls, clauses: list, logical_operator: str):
        return '(' + f' {logical_operator} '.join(clauses) + ')'

    @classmethod
    def EQUAL(cls, column, value):
        return f'{column} == {value}'

    @classmethod
    def NOT_EQUAL(cls, column, value):
        return f'{column} != {value}'

    @classmethod
    def LOWER_THAN(cls, column, value):
        return f'{column} < {value}'

    @classmethod
    def LOWER_THAN_EQUAL(cls, column, value):
        return f'{column} <= {value}'

    @classmethod
    def GREATER_THAN(cls, column, value):
        return f'{column} > {value}'

    @classmethod
    def GREATER_THAN_EQUAL(cls, column, value):
        return f'{column} >= {value}'

    @classmethod
    def IN(cls, column, value):
        return f'{column} in {value}'

    @classmethod
    def NOT_IN(cls, column, value):
        return f'{column} not in {value}'
