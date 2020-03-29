from typing import Dict, List

from toucan_connectors.condition_translator import ConditionTranslator


class MongoConditionTranslator(ConditionTranslator):
    """
    Utility class to convert a condition object into mongo $match format
    """

    @classmethod
    def join_clauses(cls, clauses: List[dict], logical_operator: str):
        return {f'${logical_operator}': clauses}

    @classmethod
    def EQUAL(cls, column, value) -> Dict[str, dict]:
        return {column: {'$eq': value}}

    @classmethod
    def NOT_EQUAL(cls, column, value) -> Dict[str, dict]:
        return {column: {'$ne': value}}

    @classmethod
    def LOWER_THAN(cls, column, value) -> Dict[str, dict]:
        return {column: {'$lt': value}}

    @classmethod
    def LOWER_THAN_EQUAL(cls, column, value) -> Dict[str, dict]:
        return {column: {'$lte': value}}

    @classmethod
    def GREATER_THAN(cls, column, value) -> Dict[str, dict]:
        return {column: {'$gt': value}}

    @classmethod
    def GREATER_THAN_EQUAL(cls, column, value) -> Dict[str, dict]:
        return {column: {'$gte': value}}

    @classmethod
    def IN(cls, column, values) -> Dict[str, dict]:
        return {column: {'$in': values}}

    @classmethod
    def NOT_IN(cls, column, values) -> Dict[str, dict]:
        return {column: {'$nin': values}}

    @classmethod
    def MATCHES(cls, column, value) -> Dict[str, dict]:
        return {column: {'$regex': value}}

    @classmethod
    def NOT_MATCHES(cls, column, value) -> Dict[str, dict]:
        return {column: {'$not': {'$regex': value}}}

    @classmethod
    def IS_NULL(cls, column, value=None) -> Dict[str, dict]:
        return {column: {'$exists': False}}

    @classmethod
    def IS_NOT_NULL(cls, column, value=None) -> Dict[str, dict]:
        return {column: {'$exists': True}}
