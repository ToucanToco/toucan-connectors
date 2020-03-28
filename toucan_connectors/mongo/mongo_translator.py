from toucan_connectors.common import ConditionTranslator


class MongoConditionTranslator(ConditionTranslator):
    """
    Utility class to convert a condition object into mongo $match format
    """

    @classmethod
    def join_clauses(cls, clauses: list, logical_operator: str):
        return {f'${logical_operator}': clauses}

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
