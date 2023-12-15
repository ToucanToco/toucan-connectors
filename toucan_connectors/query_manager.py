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
        logger.debug("call execute method")
        if isinstance(execute_method, types.MethodType) or isinstance(execute_method, types.FunctionType):
            result = execute_method(connection, query, parameters)
            return result
        else:
            raise TypeError("execute_method is not callable")

    def execute(self, execute_method, connection, query: str, query_parameters: Optional[Dict] = None):
        result = QueryManager._execute(execute_method, connection, query, query_parameters)
        return result

    @staticmethod
    def _describe(describe_method, connection, query: str):
        logger.debug("call describe method")
        if isinstance(describe_method, types.MethodType) or isinstance(describe_method, types.FunctionType):
            result = describe_method(connection, query)
            return result
        else:
            raise TypeError("describe_method is not callable")

    def describe(self, describe_method, connection, query: str):
        result = QueryManager._describe(describe_method, connection, query)
        return result
