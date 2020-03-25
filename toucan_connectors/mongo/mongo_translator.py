from enum_switch import Switch

from toucan_connectors.common import ConditionOperator


def permission_condition_to_mongo_clause(condition: dict) -> dict:
    """
    Convert a SimpleCondition to it's maongo clause equivalent.
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

    mongo_operator_mapping = MongoOperatorMapping(ConditionOperator)
    generate_mongo_clause = mongo_operator_mapping(operator)
    return generate_mongo_clause(column, value)


def permission_conditions_to_mongo_query(group: dict) -> dict:
    if 'or' in group:
        if isinstance(group['or'], list):
            return {'$or': [permission_conditions_to_mongo_query(group) for group in group['or']]}
        else:
            raise ValueError("'or' value must be an array")
    elif 'and' in group:
        if isinstance(group['and'], list):
            return {'$and': [permission_conditions_to_mongo_query(group) for group in group['and']]}
        else:
            raise ValueError("'and' value must be an array")
    else:
        return permission_condition_to_mongo_clause(group)


class MongoOperatorMapping(Switch):
    def EQUAL(self):
        return lambda column, value: {column: {'$eq': value}}

    def NOT_EQUAL(self):
        return lambda column, value: {column: {'$ne': value}}

    def LOWER_THAN(self):
        return lambda column, value: {column: {'$lt': value}}

    def LOWER_THAN_EQUAL(self):
        return lambda column, value: {column: {'$lte': value}}

    def GREATER_THAN(self):
        return lambda column, value: {column: {'$gt': value}}

    def GREATER_THAN_EQUAL(self):
        return lambda column, value: {column: {'$gte': value}}

    def IN(self):
        return lambda column, value: {column: {'$in': value}}

    def NOT_IN(self):
        return lambda column, value: {column: {'$nin': value}}

    def MATCHES(self):
        return lambda column, value: {column: {'$regex': value}}

    def NOT_MATCHES(self):
        return lambda column, value: {column: {'$not': {'$regex': value}}}

    def IS_NULL(self):
        return lambda column, value: {column: {'$exists': False}}

    def IS_NOT_NULL(self):
        return lambda column, value: {column: {'$exists': True}}
