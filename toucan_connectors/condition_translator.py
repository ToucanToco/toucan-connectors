from abc import ABC, abstractmethod
from enum import Enum


class ConditionOperator(str, Enum):
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


class ConditionTranslator(ABC):
    """
    Class with utilities methods to translate data condition from a
    dictionnary to clauses that can be applied to filter data.

    The main method is `translate`.
    """

    @classmethod
    def translate(cls, condition: dict, **kwargs):
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
                    [cls.translate(condition, **kwargs) for condition in condition['or']], 'or'
                )
            else:
                raise ValueError("'or' value must be an array")
        elif 'and' in condition:
            if isinstance(condition['and'], list):
                return cls.join_clauses(
                    [cls.translate(condition, **kwargs) for condition in condition['and']], 'and'
                )
            else:
                raise ValueError("'and' value must be an array")
        else:
            return cls.generate_clause(**condition, **kwargs)

    @classmethod
    @abstractmethod
    def join_clauses(cls, clauses: list, logical_operator: str):
        """
        Join multiple clauses with `and` or `or`.
        """
        raise NotImplementedError

    @classmethod
    def generate_clause(
        cls, column: str, operator: str, value, enclosing_field_char='', enclosing_value_char=''
    ):
        condition_operator = ConditionOperator(operator)
        clause_generator_for_operator = getattr(cls, condition_operator.name)

        if isinstance(value, str):
            value = f'{enclosing_value_char}{value}{enclosing_value_char}'

        return clause_generator_for_operator(
            f'{enclosing_field_char}{column}{enclosing_field_char}', value
        )

    # Operators

    @classmethod
    @abstractmethod
    def EQUAL(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def NOT_EQUAL(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def LOWER_THAN(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def LOWER_THAN_EQUAL(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def GREATER_THAN(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def GREATER_THAN_EQUAL(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def IN(cls, column, values):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def NOT_IN(cls, column, values):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def MATCHES(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def NOT_MATCHES(cls, column, value):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def IS_NULL(cls, column, value=None):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def IS_NOT_NULL(cls, column, value=None):
        raise NotImplementedError
