from toucan_connectors.common import ConditionOperator, ConditionTranslator


class PandasConditionTranslator(ConditionTranslator):
    @classmethod
    def translate(cls, conditions: dict, enclosing_field_char='`') -> str:
        """
        Convert a conditions object into pandas.query format
        """
        if 'or' in conditions:
            if isinstance(conditions['or'], list):
                pandas_query = ' or '.join(
                    [cls.translate(conditions) for conditions in conditions['or']]
                )
                return f'({pandas_query})'
            else:
                raise ValueError("'or' value must be an array")
        elif 'and' in conditions:
            if isinstance(conditions['and'], list):
                pandas_query = ' and '.join(
                    [cls.translate(conditions) for conditions in conditions['and']]
                )
                return f'({pandas_query})'
            else:
                raise ValueError("'and' value must be an array")
        else:
            return cls.condition_to_clause(conditions, enclosing_field_char)

    @classmethod
    def condition_to_clause(cls, condition: dict, enclosing_field_char='`') -> str:
        """
        Convert a simple condition to it's pandas clause equivalent.
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
        generate_clause = getattr(cls, operator.name)()
        return generate_clause(
            f'{enclosing_field_char}{column}{enclosing_field_char}',
            f'{enclosing_value_char}{value}{enclosing_value_char}',
        )

    @classmethod
    def EQUAL(cls):
        return lambda column, value: f'{column} == {value}'

    @classmethod
    def NOT_EQUAL(cls):
        return lambda column, value: f'{column} != {value}'

    @classmethod
    def LOWER_THAN(cls):
        return lambda column, value: f'{column} < {value}'

    @classmethod
    def LOWER_THAN_EQUAL(cls):
        return lambda column, value: f'{column} <= {value}'

    @classmethod
    def GREATER_THAN(cls):
        return lambda column, value: f'{column} > {value}'

    @classmethod
    def GREATER_THAN_EQUAL(cls):
        return lambda column, value: f'{column} >= {value}'

    @classmethod
    def IN(cls):
        return lambda column, value: f'{column} in {value}'

    @classmethod
    def NOT_IN(cls):
        return lambda column, value: f'{column} not in {value}'

    @classmethod
    def MATCHES(cls):
        raise NotImplementedError(f'Operator not implemented in pandas (MATCHES)')

    @classmethod
    def NOT_MATCHES(cls):
        raise NotImplementedError(f'Operator not implemented in pandas (NOT_MATCHES)')

    @classmethod
    def IS_NULL(cls):
        raise NotImplementedError(f'Operator not implemented in pandas (IS_NULL)')

    @classmethod
    def IS_NOT_NULL(cls):
        raise NotImplementedError(f'Operator not implemented in pandas (IS_NOT_NULL)')
