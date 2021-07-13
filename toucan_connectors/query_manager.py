import logging
import types
from typing import Dict, Optional

from toucan_connectors import DataSlice

logger = logging.getLogger(__name__)


class QueryManager:
    def __init__(self, **kwargs):
        self.query: Dict[str, DataSlice] = {}

    @staticmethod
    def _execute(execute_method, connection, query, parameters):
        logger.debug('call execute method')
        if isinstance(execute_method, types.FunctionType):
            result = execute_method(connection, query, parameters)
            return result
        else:
            raise Exception('execute_method is not callable')

    def execute(self, execute_method, connection, query, parameters: Optional[Dict]):
        logger.debug('execute query if query is not in cache')
        # cast query with parameters to hash
        hash_query = hash(query)
        hash_parameters = hash(parameters)
        hash_all = str(hash_query) + '_' + str(hash_parameters)

        if hash_query in self.query:
            return self.query[hash_all]
        else:
            result = QueryManager._execute(execute_method, connection, query, parameters)
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
