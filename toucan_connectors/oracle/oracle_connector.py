import pandas as pd
import cx_Oracle

from toucan_connectors.abstract_connector import AbstractConnector


class OracleConnector(AbstractConnector, type='oracle'):
    """ A back-end connector to retrieve data from a Oracle database """

    def __init__(self, *, host, user=None, password=None,
                 db=None, port=None, mode=None, encoding=None):
        if host is None:
            raise MissingHostParameter('You need to give a hostname and a host'
                                       ' in order to connect')
        self.params = {
            'user': user,
            'password': password,
            'dsn': f'{host}:{port}/{db}',
            'mode': mode,
            'encoding': encoding
        }
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = cx_Oracle.connect(**self.params)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        self.connection.close()

    def _get_df(self, config):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].

        """
        query = config['query']
        return pd.read_sql(query, con=self.connection)


class MissingHostParameter(Exception):
    """ raised when neither host nor hostname is passed as an argument """
