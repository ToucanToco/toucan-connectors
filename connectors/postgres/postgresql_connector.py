import pandas as pd
import psycopg2 as pgsql

from connectors.abstract_connector import AbstractConnector


class PostgresConnector(AbstractConnector):
    """ A back-end connector to retrieve data from a PostgresSQL database """

    def __init__(self, *, user,
                 host=None, hostname=None, charset=None, db=None, password=None,
                 port=None, connect_timeout=None):
        if host is None and hostname is None:
            raise MissingHostParameter('You need to give a host or a hostname in order to connect')
        self.params = {
            'user': user,
            'host': host,
            'hostname': hostname,
            'charset': charset,
            'database': db,
            'password': password,
            'port': port,
            'connect_timeout': connect_timeout,
        }
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = pgsql.connect(**self.params)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        self.connection.close()

    def run_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_df(self, config):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].

        """
        query = config['query']
        query_max = len(query) if len(query) < 80 else 80
        self.logger.info(f'{query[:query_max]} : executing...')
        return pd.read_sql(query, con=self.connection)


class MissingHostParameter(Exception):
    """ raised when neither host nor hostname is passed as an argument """
