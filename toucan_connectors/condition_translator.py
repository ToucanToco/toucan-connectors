import sys
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, List, TypeVar, Union

from pydantic import BaseModel

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

Number = Union[int, float]

ClauseType = TypeVar('ClauseType')


LogicalOperator = Union[Literal['and'], Literal['or']]


class ConditionOperator(Enum):
    EQUAL = 'eq'
    NOT_EQUAL = 'ne'
    LOWER_THAN = 'lt'
    LOWER_THAN_EQUAL = 'le'
    GREATER_THAN = 'gt'
    GREATER_THAN_EQUAL = 'ge'
    IN = 'in'
    NOT_IN = 'nin'
    MATCHES = 'matches'
    NOT_MATCHES = 'notmatches'
    IS_NULL = 'isnull'
    IS_NOT_NULL = 'notnull'


class ConditionModel(BaseModel):
    column: str
    operator: ConditionOperator
    value: Any = ...


class ConditionTranslator(ABC):
    """
    Class with utilities methods to translate data condition from a
    dictionnary to clauses that can be applied to filter data.

    The main method is `translate`.
    """

    @classmethod
    def translate(cls, condition: dict):
        """
        Convert a condition into a format relevant for a type of connector.

        A simple condition looks like:
            {
                'column':
                'operator':
                'value':
            }

        These base blocks can be assembled in groups with logical operators:
            {
                or: [
                    { column, operator, value },
                    { column, operator, value },
                    { and: [
                        { column, operator, value },
                        { column, operator, value }
                    ] }
                ]
            }
        """
        if 'or' in condition:
            if isinstance(condition['or'], list):
                return cls.join_clauses(
                    [cls.translate(condition) for condition in condition['or']], 'or'
                )
            else:
                raise ValueError("'or' value must be an array")
        elif 'and' in condition:
            if isinstance(condition['and'], list):
                return cls.join_clauses(
                    [cls.translate(condition) for condition in condition['and']], 'and'
                )
            else:
                raise ValueError("'and' value must be an array")
        else:
            condition_m = ConditionModel(**condition)
            clause_generator_for_operator = getattr(cls, condition_m.operator.name)

            if isinstance(condition_m.value, str):
                condition_m.value = cls.get_value_str_ref(condition_m.value)

            return clause_generator_for_operator(
                cls.get_column_ref(condition_m.column), condition_m.value
            )

    @classmethod
    @abstractmethod
    def join_clauses(cls, clauses: List[ClauseType], logical_operator: LogicalOperator):
        """
        Join multiple clauses with `and` or `or`.
        """
        raise NotImplementedError

    @classmethod
    def get_column_ref(cls, column_name: str) -> str:
        """How to refer to column in the clause"""
        return column_name

    @classmethod
    def get_value_str_ref(cls, value: str) -> str:
        """How to refer to value strings in the clause"""
        return value

    # Operators to implement
    # `column` and `value` are ref strings from `get_column_ref` and `get_value_str_ref`
    @classmethod
    @abstractmethod
    def EQUAL(cls, column: str, value: str) -> ClauseType:
        """`column` values equal to `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def NOT_EQUAL(cls, column: str, value: str) -> ClauseType:
        """`column` values not equal to `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def LOWER_THAN(cls, column: str, value: Number) -> ClauseType:
        """`column` values lower than `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def LOWER_THAN_EQUAL(cls, column: str, value: Number) -> ClauseType:
        """`column` values lower than or equal to `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def GREATER_THAN(cls, column: str, value: Number) -> ClauseType:
        """`column` values greater than `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def GREATER_THAN_EQUAL(cls, column: str, value: Number) -> ClauseType:
        """`column` values greater than or equal to `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def IN(cls, column: str, values: List[str]) -> ClauseType:
        """`column` values in `values`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def NOT_IN(cls, column: str, values: List[str]) -> ClauseType:
        """`column` values not in `values`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def MATCHES(cls, column: str, value: str) -> ClauseType:
        """`column` values match the regex `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def NOT_MATCHES(cls, column, value) -> ClauseType:
        """`column` values don't match the regex `value`"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def IS_NULL(cls, column: str, value=None) -> ClauseType:
        """`column` values are null"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def IS_NOT_NULL(cls, column: str, value=None) -> ClauseType:
        """`column` values are not null"""
        raise NotImplementedError
