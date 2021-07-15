import logging
import types
from typing import Dict, Optional

from toucan_connectors import DataSlice

logger = logging.getLogger(__name__)


class QueryManager:
    def __init__(self):
        self.query: Dict[str, DataSlice] = {}

    @staticmethod
    def _execute(execute_method, connection, query: str, parameters: Optional[Dict] = None):
        logger.debug('call execute method')
        if isinstance(execute_method, types.MethodType):
            result = execute_method(connection, query, parameters)
            return result
        else:
            raise Exception('execute_method is not callable')

    def execute(
        self, execute_method, connection, query: str, query_parameters: Optional[Dict] = None
    ):
        logger.debug('execute query if query is not in cache')
        # cast query with parameters to hash
        # hash_query = hash(query)
        # hash_parameters = hash(str(parameters))
        # hash_all = str(hash_query) + '_' + str(hash_parameters)

        # if hash_query in self.query:
        #     return self.query[hash_all]
        # else:
        result = QueryManager._execute(execute_method, connection, query, query_parameters)
        # self.query[hash_all] = result
        return result

    @staticmethod
    def fetchmany(executed_cursor):

        size = executed_cursor.arraysize
        ret = []
        while size > 0:
            row = executed_cursor.fetchone()
            if not row:
                break
            ret.append(row)
            size -= 1 if size else None

        return ret
