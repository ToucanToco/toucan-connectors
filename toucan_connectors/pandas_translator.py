from enum_switch import Switch

from toucan_connectors.common import ConditionOperator


def permission_condition_to_pandas_clause(condition: dict, enclosing_field_char='`') -> str:
    """
    Convert a SimpleCondition to it's pandas clause equivalent.
    """
    if 'operator' not in condition:
        raise KeyError('key "operator" is missing from permission condition')
    else:
        operator = ConditionOperator(condition['operator'])

    if 'column' not in condition:
        raise KeyError('key "column" is missing from permission condition')
    else:
        column = condition['column']

    if 'value' not in condition:
        raise KeyError('key "value" is missing from permission condition')
    else:
        value = condition['value']

    enclosing_value_char = "'" if isinstance(value, str) else ''
    pandas_operator_mapping = PandasOperatorMapping(ConditionOperator)
    generate_pandas_clause = pandas_operator_mapping(operator)
    return generate_pandas_clause(
        f'{enclosing_field_char}{column}{enclosing_field_char}',
        f'{enclosing_value_char}{value}{enclosing_value_char}',
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


class PandasOperatorMapping(Switch):
    def EQUAL(self):
        return lambda column, value: f'{column} == {value}'

    def NOT_EQUAL(self):
        return lambda column, value: f'{column} != {value}'

    def LOWER_THAN(self):
        return lambda column, value: f'{column} < {value}'

    def LOWER_THAN_EQUAL(self):
        return lambda column, value: f'{column} <= {value}'

    def GREATER_THAN(self):
        return lambda column, value: f'{column} > {value}'

    def GREATER_THAN_EQUAL(self):
        return lambda column, value: f'{column} >= {value}'

    def IN(self):
        return lambda column, value: f'{column} in {value}'

    def NOT_IN(self):
        return lambda column, value: f'{column} not in {value}'

    def MATCHES(self):
        raise NotImplementedError(f'Operator not implemented in pandas (MATCHES)')

    def NOT_MATCHES(self):
        raise NotImplementedError(f'Operator not implemented in pandas (NOT_MATCHES)')

    def IS_NULL(self):
        raise NotImplementedError(f'Operator not implemented in pandas (IS_NULL)')

    def IS_NOT_NULL(self):
        raise NotImplementedError(f'Operator not implemented in pandas (IS_NOT_NULL)')
