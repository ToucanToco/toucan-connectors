import pandas as pd
import psycopg2 as pgsql

from connectors.abstract_connector import MissingConnectorOption
from ..sql_connector import SQLConnector


class PostgresConnector(SQLConnector):
    """ A back-end connector to retrieve data from a PostgresSQL database """

    def __init__(self, **kwargs):
        super(PostgresConnector, self).__init__(**kwargs)
        self._check_host_or_hostname_in_connection_params()

    def _changes_normalize_args(self):
        """
        Map db keyword from etl_config to the expected keyword from the
        postgressql python library.

        Returns:
            dict: user keyword: library keyword.

        """
        return {
            'db': 'database'
        }

    def _get_required_args(self):
        return ['user']

    def _get_optional_args(self):
        return ['host', 'hostname', 'charset', 'db', 'password',
                'port', 'connect_timeout']

    def _check_host_or_hostname_in_connection_params(self):
        if 'host' not in self.connection_params and 'hostname' not in self.connection_params:
            raise MissingConnectorOption(self, 'host or hostname')

    def _get_provider_connection(self):
        return pgsql.connect(**self.connection_params)

    def get_df(self, config):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].

        """
        self.open_connection()
        query = config['query']
        query_max = len(query) if len(query) < 80 else 80
        self.logger.info('{} : executing...'.format(query[:query_max]))
        return pd.read_sql(query, con=self.connection)
