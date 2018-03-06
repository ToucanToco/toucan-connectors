# coding: utf-8
import logging
import re

import numpy as np
import pymysql

from connectors.sql_connector import SQLConnector, InvalidSQLQuery


class MySQLConnector(SQLConnector):
    logger = logging.getLogger(__name__)

    """
    A back-end connector to retrieve data from a MySQL database

    """

    def __init__(self, **kwargs):
        super(MySQLConnector, self).__init__(**kwargs)
        self.connection_params['conv'] = pymysql.converters.conversions.copy()
        self.connection_params['conv'][246] = float
        self.connection_params['charset'] = kwargs.get('charset', 'utf8mb4')

    def _get_required_args(self):
        return ['host', 'db', 'user']

    def _get_optional_args(self):
        return ['charset', 'password', 'port', 'connect_timeout']

    def _get_provider_connection(self):
        self.connection_params['cursorclass'] = pymysql.cursors.DictCursor
        return pymysql.connect(**self.connection_params)

    def _retrieve_response(self, query):
        num_rows = self.cursor.execute(query)
        if num_rows > 0:
            return self.cursor.fetchall()
        else:
            return {}

    def _clean_response(self, response):
        for elt in response:
            for k, v in elt.items():
                if v is None:
                    elt[k] = np.nan
                elif isinstance(v, bytes):
                    elt[k] = v.decode('utf8')
        return response

    def get_df(self, config):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].

        """

        # ----- Prepare -----

        self.open_connection()
        if 'query' in config:
            query = config['query']
            # Extract table name for logging purpose (see below)
            m = re.search(r"from\s*(?P<table>[^\s]+)\s*(where|order by|group by|limit)?", query,
                          re.I)
            table = m.group('table')
        else:
            table = config['table']
            query = 'select * from {}'.format(table)
        MySQLConnector.logger.debug('Executing query : {}'.format(query))
        # list used because we cannot reassign a variable to update the dataframe.
        # After the merge: append the new DataFrame and remove the pop the first
        # element.
        lres = [(self._df_from_query(query))]
        MySQLConnector.logger.info(table + ': dumped first DataFrame')

        # ----- Merge -----

        # Avoid circular dependency (unnecessary merges and can add and rename
        # to many columns.
        infos = []
        has_been_merged = {table}
        foreign_keys_append = self.get_foreign_key_info(table)
        if len(foreign_keys_append) > 0:
            for keys in foreign_keys_append:
                infos.append(keys)
        while infos:
            table_info = infos.pop()

            if table_info is None:
                continue
            elif table_info['f_table'] in has_been_merged:
                continue

            MySQLConnector.logger.info(
                ''.join([table, ' <> found foreign key: ', table_info['f_key'],
                         ' inside ', table_info['f_table']]))

            f_df = self._df_from_query('select * from {}'.format(table_info['f_table']))
            suffixes = ('_' + table, '_' + table_info['f_table'])
            lres.append(
                self._merge_drop(lres[0],
                                 f_df,
                                 suffixes,
                                 table_info['f_key'],
                                 table_info['f_table'],
                                 table_info['f_table_key'])
            )

            if lres:
                del lres[0]

            has_been_merged.add(table_info['f_table'])

            table = table_info['f_table']
            foreign_keys_append = self.get_foreign_key_info(table)
            if len(foreign_keys_append) > 0:
                for keys in foreign_keys_append:
                    infos.append(keys)

        return lres.pop()

    def _merge_drop(self, df, f_df, suffixes, f_key, f_table, f_table_key):
        """
        Merge two DataFrames and drop unnecessary column.
        Args:
            df: base DataFrame
            f_df: DataFrame from foreign table
            suffixes: suffixes for merge
            f_key: foreign key
            f_table: foreign table
            f_table_key: foreign table's key

        Returns: Bigger DataFrame.

        """
        tmp_df = df.merge(f_df,
                          left_on=f_key,
                          right_on=f_table_key,
                          how='outer',
                          suffixes=suffixes)

        MySQLConnector.logger.info('Merged two DataFrames...')
        if f_key != f_table_key:
            if f_table_key not in tmp_df.columns:
                MySQLConnector.logger.info(
                    ''.join([' and dropped one column ', f_table_key, suffixes[0]]))
                return tmp_df.drop(f_table_key + suffixes[0], axis=1)
            MySQLConnector.logger.info(' no column dropped')
            return tmp_df.drop(f_table_key, axis=1)
        return tmp_df

    def get_foreign_keys(self, table_name):
        res = []
        tmp_info = self.get_foreign_key_info(table_name)
        while tmp_info:
            tmp_info = self.get_foreign_key_info(table_name)
            if tmp_info is not None:
                res.append(tmp_info)
        return res

    def execute(self, query):
        res = self.cursor.execute(query)
        self.description = self.cursor.description
        return res

    def execute_and_fetchall(self, query):
        self.cursor.execute(query)
        self.description = self.cursor.description
        return self.cursor.fetchall()

    def fetchall(self):
        MySQLConnector.logger.info('fetch all')
        return self.cursor.fetchall()

    def close(self):
        self.connection.close()

    @staticmethod
    def extract_info(fetch_all):
        """
        Extract the key, table name and key in the foreign table from a
        line. It must be the line that has the 'FOREIGN KEY' string. A
        find must have been done before calling this method.
        Args:
            fetch_all: string to parse

        Returns: list of dicts with 'f_key', 'f_table', 'f_table_key' keys.

        """
        idx = 0
        res = []

        while idx >= 0:
            info = dict()

            MySQLConnector.logger.info(
                'start searching for foreign key.'
            )

            info['f_key'], idx = \
                MySQLConnector.extract_info_word(
                    fetch_all, idx, ['FOREIGN', 'KEY']
                )
            if idx == -1:
                MySQLConnector.logger.info('No (other) foreign key.')
                return res

            info['f_table'], idx = \
                MySQLConnector.extract_info_word(
                    fetch_all, idx, ['REFERENCES']
                )
            if idx == -1:
                MySQLConnector.logger.error(
                    ''.join(['Foreign key ', info['f_key'], ' found but no REFERENCES found.'])
                )
                return res

            info['f_table_key'], idx = \
                MySQLConnector.extract_info_word(
                    fetch_all, idx, []
                )
            if idx == -1:
                MySQLConnector.logger.error(
                    ''.join(['Foreign key ', info['f_key'],
                             ' and REFERENCES found but no foreign table key.'])
                )
                return res

            res.append(info)

        MySQLConnector.logger.info(
            'Got all foreign key information.'
        )
        return res

    @staticmethod
    def extract_info_word(line, start, words_to_match):
        """
        Extract an information such as a table or a key name from a string.
        Parse the string word by word and find the wanted information after
         some words to match (eg. find a key after FOREIGN KEY)
        Args:
            line: string to parse
            start: start index
            words_to_match: list of words to match before the wanted information.

        Returns: wanted information as a string

        """
        idx = start
        len_line = len(line)

        curr_idx_to_match = 0
        len_words_to_match = len(words_to_match)

        while idx < len_line:
            word, idx = MySQLConnector._get_word(line, idx)
            if curr_idx_to_match == len_words_to_match:
                return word, idx
            if word.upper() == words_to_match[curr_idx_to_match]:
                curr_idx_to_match = curr_idx_to_match + 1

        return ('', -1)

    @staticmethod
    def _get_word(line, start):
        """
        Extract the first word found starting from start index
        Args:
            line: line to parse
            start: start index

        Returns: first word found.

        """

        # TODO: improve this...

        def valid_char(word):
            return word.isalnum() or word == '_'

        idx_start = start
        len_line = len(line)

        # Remove non alphanumeric characters
        while idx_start < len_line and not valid_char(line[idx_start]):
            idx_start = idx_start + 1
        if idx_start == len_line:
            return ('', idx_start)

        # Get the word
        idx_end = idx_start
        while idx_end < len_line and valid_char(line[idx_end]):
            idx_end = idx_end + 1

        if idx_end == 0 or idx_end == len_line:
            return ('', idx_end)
        return (line[idx_start:idx_end], idx_end)

    def get_tables_name(self):
        """
        Execute a show table query to retrieve the table names

        Returns: list of table names.

        """
        return [list(table.values())[0] for table in self.execute_and_fetchall('show tables')]

    def get_foreign_key_info(self, table_name):
        """
        Get the foreign key information from a table: foreign key inside the
        table, foreign table, key inside that table.
        Args:
            table_name: table name...

        Returns: list of dicts ('f_key', 'f_table', 'f_table_key')

        """
        # TODO: see if there is a more efficient way to do this
        fetch_all_list = self.execute_and_fetchall('show create table ' + table_name)

        keys = list(fetch_all_list[0].keys())
        if 'Create Table' in keys:
            fetch_all = fetch_all_list[0]['Create Table']
        elif 'Create View' in keys:
            fetch_all = fetch_all_list[0]['Create View']
        else:
            raise InvalidSQLQuery(keys)
        return MySQLConnector.extract_info(fetch_all)
