from toucan_connectors.common import ConditionOperator, ConditionTranslator


class MongoConditionTranslator(ConditionTranslator):
    @classmethod
    def translate(cls, conditions: dict) -> dict:
        """
        Convert a conditions object into mongo $match clauses
        """
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

        generate_clause = getattr(cls, operator.name)()
        return generate_clause(column, value)

    @classmethod
    def EQUAL(self):
        return lambda column, value: {column: {'$eq': value}}

    @classmethod
    def NOT_EQUAL(self):
        return lambda column, value: {column: {'$ne': value}}

    @classmethod
    def LOWER_THAN(self):
        return lambda column, value: {column: {'$lt': value}}

    @classmethod
    def LOWER_THAN_EQUAL(self):
        return lambda column, value: {column: {'$lte': value}}

    @classmethod
    def GREATER_THAN(self):
        return lambda column, value: {column: {'$gt': value}}

    @classmethod
    def GREATER_THAN_EQUAL(self):
        return lambda column, value: {column: {'$gte': value}}

    @classmethod
    def IN(self):
        return lambda column, value: {column: {'$in': value}}

    @classmethod
    def NOT_IN(self):
        return lambda column, value: {column: {'$nin': value}}

    @classmethod
    def MATCHES(self):
        return lambda column, value: {column: {'$regex': value}}

    @classmethod
    def NOT_MATCHES(self):
        return lambda column, value: {column: {'$not': {'$regex': value}}}

    @classmethod
    def IS_NULL(self):
        return lambda column, value=None: {column: {'$exists': False}}

    @classmethod
    def IS_NOT_NULL(self):
        return lambda column, value=None: {column: {'$exists': True}}
