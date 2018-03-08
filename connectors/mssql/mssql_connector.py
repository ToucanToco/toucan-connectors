import pymssql

import pandas as pd

from connectors.abstract_connector import AbstractConnector


class MSSQLConnector(AbstractConnector):
    """ A back-end connector to retrieve data from a MSSQL database """

    def __init__(self, *, host, user,
                 db=None, password=None, port=None, connect_timeout=None):
        self.params = {
            'server': host,
            'user': user,
            'database': db,
            'password': password,
            'port': port,
            'login_timeout': connect_timeout,
            'as_dict': True
        }
        # remove None value
        self.params = {k: v for k, v in self.params.items() if v is not None}
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = pymssql.connect(**self.params)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        self.connection.close()

    def run_query(self, query):
        """
        Args:
            query: query (SQL) to execute

        Returns: DataFrame

        """
        return pd.read_sql(query, con=self.connection)

    def get_df(self, config):
        """
        Returns: DataFrame from provided query
        
        """
        query = config['query']
        self.logger.info(f'{query} : executing...')
        return self.run_query(query.encode('utf8'))
