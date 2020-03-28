from toucan_connectors.common import ConditionOperator, ConditionTranslator


class MongoConditionTranslator(ConditionTranslator):
    """
    Utility class to convert a condition object into mongo $match format
    """

    @classmethod
    def translate(cls, conditions: dict) -> dict:
        if 'or' in conditions:
            if isinstance(conditions['or'], list):
                return {'$or': [cls.translate(conditions) for conditions in conditions['or']]}
            else:
                raise ValueError("'or' value must be an array")
        elif 'and' in conditions:
            if isinstance(conditions['and'], list):
                return {'$and': [cls.translate(conditions) for conditions in conditions['and']]}
            else:
                raise ValueError("'and' value must be an array")
        else:
            return cls.condition_to_clause(conditions)

    @classmethod
    def condition_to_clause(cls, condition: dict) -> dict:
        """
        Convert a SimpleCondition to it's mongo clause equivalent.
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

        generate_clause = getattr(cls, operator.name)
        return generate_clause(column, value)

    @classmethod
    def EQUAL(cls, column, value):
        return {column: {'$eq': value}}

    @classmethod
    def NOT_EQUAL(cls, column, value):
        return {column: {'$ne': value}}

    @classmethod
    def LOWER_THAN(cls, column, value):
        return {column: {'$lt': value}}

    @classmethod
    def LOWER_THAN_EQUAL(cls, column, value):
        return {column: {'$lte': value}}

    @classmethod
    def GREATER_THAN(cls, column, value):
        return {column: {'$gt': value}}

    @classmethod
    def GREATER_THAN_EQUAL(cls, column, value):
        return {column: {'$gte': value}}

    @classmethod
    def IN(cls, column, values):
        return {column: {'$in': values}}

    @classmethod
    def NOT_IN(cls, column, values):
        return {column: {'$nin': values}}

    @classmethod
    def MATCHES(cls, column, value):
        return {column: {'$regex': value}}

    @classmethod
    def NOT_MATCHES(cls, column, value):
        return {column: {'$not': {'$regex': value}}}

    @classmethod
    def IS_NULL(cls, column, value=None):
        return {column: {'$exists': False}}

    @classmethod
    def IS_NOT_NULL(cls, column, value=None):
        return {column: {'$exists': True}}
