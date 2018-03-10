import re

import numpy as np
import pandas as pd
import pymysql

from toucan_connectors.abstract_connector import AbstractConnector, InvalidQuery


class MySQLConnector(AbstractConnector):
    """ A back-end connector to retrieve data from a MySQL database """

    def __init__(self, *, host, user, db,
                 charset='utf8mb4', password=None, port=None, connect_timeout=None):
        conv = pymysql.converters.conversions.copy()
        conv[246] = float
        self.params = {
            'host': host,
            'user': user,
            'database': db,
            'charset': charset,
            'password': password,
            'port': port,
            'connect_timeout': connect_timeout,
            'conv': conv,
            'cursorclass': pymysql.cursors.DictCursor
        }
        # remove None value
        self.params = {k: v for k, v in self.params.items() if v is not None}
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = pymysql.connect(**self.params)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        self.connection.close()

    @staticmethod
    def clean_response(response):
        for elt in response:
            for k, v in elt.items():
                if v is None:
                    elt[k] = np.nan
                elif isinstance(v, bytes):
                    elt[k] = v.decode('utf8')
        return response

    def run_query(self, query):
        num_rows = self.cursor.execute(query)
        response = self.cursor.fetchall() if num_rows > 0 else {}
        return self.clean_response(response)

    def execute_and_fetchall(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    @staticmethod
    def _merge_drop(df, f_df, suffixes, f_key, f_table_key):
        """
        Merge two DataFrames and drop unnecessary column.
        Args:
            df: base DataFrame
            f_df: DataFrame from foreign table
            suffixes: suffixes for merge
            f_key: foreign key
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
                MySQLConnector.logger.info(f' and dropped one column {f_table_key}{suffixes[0]}')
                return tmp_df.drop(f_table_key + suffixes[0], axis=1)
            MySQLConnector.logger.info(' no column dropped')
            return tmp_df.drop(f_table_key, axis=1)
        return tmp_df

    def _df_from_query(self, query):
        """
        Args:
            query: query (SQL) to execute

        Returns: DataFrame

        """
        return pd.read_sql(query, con=self.connection)

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
            info = {}
            MySQLConnector.logger.info('start searching for foreign key.')
            info['f_key'], idx = MySQLConnector.extract_info_word(fetch_all, idx,
                                                                  ['FOREIGN', 'KEY'])
            if idx == -1:
                MySQLConnector.logger.info('No (other) foreign key.')
                return res

            info['f_table'], idx = MySQLConnector.extract_info_word(fetch_all, idx, ['REFERENCES'])
            if idx == -1:
                MySQLConnector.logger.error(f"Foreign key {info['f_key']}, "
                                            f"found but no REFERENCES found.")
                return res

            info['f_table_key'], idx = MySQLConnector.extract_info_word(fetch_all, idx, [])
            if idx == -1:
                MySQLConnector.logger.error(f"Foreign key {info[f'_key']} "
                                            f"and REFERENCES found but no foreign table key.")
                return res
            res.append(info)

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

        return '', -1

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
            return '', idx_start

        # Get the word
        idx_end = idx_start
        while idx_end < len_line and valid_char(line[idx_end]):
            idx_end = idx_end + 1

        if idx_end == 0 or idx_end == len_line:
            return '', idx_end
        return line[idx_start:idx_end], idx_end

    def get_foreign_key_info(self, table_name):
        """
        Get the foreign key information from a table: foreign key inside the
        table, foreign table, key inside that table.
        Args:
            table_name: table name...

        Returns: list of dicts ('f_key', 'f_table', 'f_table_key')

        """
        # TODO: see if there is a more efficient way to do this
        fetch_all_list = self.execute_and_fetchall(f'show create table {table_name}')

        keys = list(fetch_all_list[0].keys())
        if 'Create Table' in keys:
            fetch_all = fetch_all_list[0]['Create Table']
        elif 'Create View' in keys:
            fetch_all = fetch_all_list[0]['Create View']
        else:
            raise InvalidQuery(keys)
        return MySQLConnector.extract_info(fetch_all)

    def get_df(self, config):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].

        """

        # ----- Prepare -----

        if 'query' in config:
            query = config['query']
            # Extract table name for logging purpose (see below)
            m = re.search(r"from\s*(?P<table>[^\s]+)\s*(where|order by|group by|limit)?",
                          query, re.I)
            table = m.group('table')
        else:
            table = config['table']
            query = f'select * from {table}'
        MySQLConnector.logger.debug(f'Executing query : {query}')
        # list used because we cannot reassign a variable to update the dataframe.
        # After the merge: append the new DataFrame and remove the pop the first
        # element.
        lres = [(self._df_from_query(query))]
        MySQLConnector.logger.info(f'{table} : dumped first DataFrame')

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

            MySQLConnector.logger.info(f"{table} <> found foreign key: {table_info['f_key']} "
                                       f"inside {table_info['f_table']}")

            f_df = self._df_from_query(f'select * from {table_info["f_table"]}')
            suffixes = ('_' + table, '_' + table_info['f_table'])
            lres.append(
                self._merge_drop(lres[0],
                                 f_df,
                                 suffixes,
                                 table_info['f_key'],
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
