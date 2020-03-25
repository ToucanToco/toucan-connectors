from enum import Enum


def permission_condition_to_pandas_clause(condition: dict, enclosing_field_char='`') -> str:
    """
    Convert a permission condition to it's pandas clause equivalent.

    :param      condition:             The condition tree
    :type       condition:             dict
    :param      enclosing_field_char:  The enclosing field character
    :type       enclosing_field_char:  string

    :returns:   The pandas clause
    :rtype:     str
    """
    if 'operator' not in condition:
        raise KeyError('key "operator" is missing from permission condition')
    elif 'column' not in condition:
        raise KeyError('key "column" is missing from permission condition')
    elif 'value' not in condition:
        raise KeyError('key "value" is missing from permission condition')
    else:
        column = condition['column']
        operator = PandasOperatorMapping.from_identifier(condition['operator'])
        if operator is None:
            raise ValueError(f'Unsupported operator:{condition["operator"]}')
        values = condition['value']
        enclosing_value_char = "'" if isinstance(values, str) else ''
        return operator.to_clause(
            f'{enclosing_field_char}{column}{enclosing_field_char}',
            f'{enclosing_value_char}{values}{enclosing_value_char}',
        )


def permission_conditions_to_pandas_query(group: dict, enclosing_field_char='`') -> str:
    """
    Convert a group of permission condition in a string format

    :param      group:                 The group of permission
    :type       group:                 dict
    :param      enclosing_field_char:  The enclosing field character for the column names
    :type       enclosing_field_char:  string

    :returns:   The panda format.
    :rtype:     str
    """
    if 'or' in group:
        if isinstance(group['or'], list):
            conditions = ' or '.join(
                [permission_conditions_to_pandas_query(group) for group in group['or']]
            )
            return f'({conditions})'
        else:
            raise ValueError("'or' value must be an array")
    elif 'and' in group:
        if isinstance(group['and'], list):
            conditions = ' and '.join(
                [permission_conditions_to_pandas_query(group) for group in group['and']]
            )
            return f'({conditions})'
        else:
            raise ValueError("'and' value must be an array")
    else:
        return permission_condition_to_pandas_clause(group, enclosing_field_char)


class PandasOperatorMapping(Enum):
    EQUAL = {'identifier': 'eq', 'template': '{column} == {value}'}
    NOT_EQUAL = {'identifier': 'ne', 'template': '{column} != {value}'}
    LOWER_THAN = {'identifier': 'lt', 'template': '{column} < {value}'}
    LOWER_THAN_EQUAL = {'identifier': 'le', 'template': '{column} <= {value}'}
    GREATER_THAN = {'identifier': 'gt', 'template': '{column} > {value}'}
    GREATER_THAN_EQUAL = {'identifier': 'ge', 'template': '{column} >= {value}'}
    IN = {'identifier': 'in', 'template': '{column} in {value}'}
    NOT_IN = {'identifier': 'nin', 'template': '{column} not in {value}'}

    def to_clause(self, column, value):
        return self.value['template'].format(column=column, value=value)

    @classmethod
    def from_identifier(cls, operator: str):
        return next((item for item in cls if operator == item.value['identifier']), None)
