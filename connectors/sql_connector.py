import logging
from abc import abstractmethod

import collections
import pandas as pd
from copy import copy

from .abstract_connector import AbstractConnector


class SQLConnector(AbstractConnector):
    """
    Base class for back-end SQL connectors which implements common basic functions

    Each subclass must implement _get_required_args method for this parent constructor to validate them

    Args:
        name (string): name of the connector
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.connection_params = self._compute_connection_params()
        self.connection_params = self._normalize_args()

        self.connection = None
        self.cursor = None
        self.logger = logging.getLogger(__name__)

    def _compute_connection_params(self):
        required = {arg: getattr(self, arg) for arg in self._get_required_args()}
        optional_args = [opt for opt in self._get_optional_args() if hasattr(self, opt)]
        required.update({opt: getattr(self, opt) for opt in optional_args})
        return required

    def is_connected(self):
        try:
            self.query("SELECT 1 as one;")
        except SQLException:
            return False
        return True

    def open_connection(self):
        if not self.connection:
            try:
                self.connection = self._get_provider_connection()
                self.cursor = self.connection.cursor()
                self.logger.info('connection opened')
                return self.connection
            except Exception as e:
                msg = str(e)

                self.logger.error('open connection error: ' + str(msg))
                raise UnableToConnectToDatabaseException(msg)

    def close_connection(self):
        self.connection.close()
        self.connection = None
        self.logger.info('connection closed')

    def _changes_normalize_args(self):
        """
        Returns:
            dict: empty dict (default method, should be override in subclasses)
        """
        return {}

    def _normalize_args(self):
        """
        Normalize connection params, by either:
            * renaming keys (if changes = {'db': 'database'})
            * evaluating a function on values (if changes = {'host': somefunction}).
        Returns:
            dict: copy of self.connection_params with normalized keys.
        """
        normalized = copy(self.connection_params)
        for k, v in list(self._changes_normalize_args().items()):
            if isinstance(v, str):
                normalized[v] = self.connection_params[k]
                del normalized[k]
            elif isinstance(v, collections.Callable):
                normalized[k] = v(self.connection_params)

        return normalized

    def query(self, query, fields=None):
        self.open_connection()
        try:
            return self._clean_response(self._retrieve_response(query))
        except Exception as e:
            msg = str(e)
            self.logger.error(''.join(['query error: ', query, ' <> msg: ', msg]))
            raise InvalidSQLQuery(msg)

    @abstractmethod
    def _get_provider_connection(self):
        raise NotImplementedError

    def _retrieve_response(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def _clean_response(self, response):
        return response

    def _df_from_query(self, query):
        """
        Args:
            query: query (SQL) to execute

        Returns: DataFrame

        """
        return pd.read_sql(query, con=self.connection)


class SQLException(Exception):
    def __init__(self, msg):
        self.msg = msg


class UnableToConnectToDatabaseException(SQLException):
    """ Raised when connection fails to open """


class InvalidSQLQuery(SQLException):
    """ Raised when the Hana query is invalid """


class EmptySQLResponse(SQLException):
    """ Raised when the response to the Hana query is empty """
