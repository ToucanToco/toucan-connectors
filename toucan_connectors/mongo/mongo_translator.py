from enum import Enum


def permission_condition_to_mongo_clause(condition: dict) -> dict:
    if 'operator' not in condition:
        raise KeyError('key "operator" is missing from permission condition')
    else:
        operator = MongoOperatorMapping.from_identifier(condition['operator'])
        if operator is None:
            raise ValueError(f'Unsupported operator:{condition["operator"]}')

    if 'column' not in condition:
        raise KeyError('key "column" is missing from permission condition')
    else:
        column = condition['column']

    if 'value' not in condition:
        raise KeyError('key "value" is missing from permission condition')
    else:
        value = condition['value']

    return operator.to_clause(column, value)


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


class MongoOperatorMapping(Enum):
    EQUAL = {'identifier': 'eq', 'clause': lambda column, value: {column: {'$eq': value}}}
    NOT_EQUAL = {'identifier': 'ne', 'clause': lambda column, value: {column: {'$ne': value}}}
    LOWER_THAN = {'identifier': 'lt', 'clause': lambda column, value: {column: {'$lt': value}}}
    LOWER_THAN_EQUAL = {
        'identifier': 'le',
        'clause': lambda column, value: {column: {'$lte': value}},
    }
    GREATER_THAN = {'identifier': 'gt', 'clause': lambda column, value: {column: {'$gt': value}}}
    GREATER_THAN_EQUAL = {
        'identifier': 'ge',
        'clause': lambda column, value: {column: {'$gte': value}},
    }
    IN = {'identifier': 'in', 'clause': lambda column, value: {column: {'$in': value}}}
    NOT_IN = {'identifier': 'nin', 'clause': lambda column, value: {column: {'$nin': value}}}
    MATCHES = {
        'identifier': 'matches',
        'clause': lambda column, value: {column: {'$regex': value}},
    }
    NOT_MATCHES = {
        'identifier': 'notmatches',
        'clause': lambda column, value: {column: {'$not': {'$regex': value}}},
    }
    IS_NULL = {
        'identifier': 'isnull',
        'clause': lambda column, value: {column: {'$exists': False}},
    }
    IS_NOT_NULL = {
        'identifier': 'notnull',
        'clause': lambda column, value: {column: {'$exists': True}},
    }

    def to_clause(self, column, value):
        return self.value['clause'](column, value)

    @classmethod
    def from_identifier(cls, operator: str):
        return next((item for item in cls if operator == item.value['identifier']), None)
