import re
from contextlib import suppress
from typing import Optional

import numpy as np
import pymysql
from pydantic import Field, SecretStr, constr, create_model
from pymysql.constants import CR, ER

from toucan_connectors.common import ConnectorStatus, pandas_read_sql
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum


class MySQLDataSource(ToucanDataSource):
    """
    Either `query` or `table` are required, both at the same time are not supported.
    """

    database: str = Field(..., description='The name of the database you want to query')
    table: constr(min_length=1) = Field(
        None,
        description='The name of the data table that you want to '
        'get (equivalent to "SELECT * FROM '
        'your_table")',
    )
    query: constr(min_length=1) = Field(
        None,
        description='You can write a custom query against your '
        'database here. It will take precedence over '
        'the "table" parameter above',
        widget='sql',
    )
    follow_relations: bool = Field(
        False,
        description='Whether you want to perform automatic inner '
        'joins of related tables based on every foreign '
        'key found in the queried table (left table of '
        'the join). As a general rule, you should not '
        'need to activate this parameter',
    )

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get('query')
        table = data.get('table')
        if query is None and table is None:
            raise ValueError("'query' or 'table' must be set")
        elif query is not None and table is not None:
            raise ValueError("Only one of 'query' or 'table' must be set")

    @classmethod
    def get_form(cls, connector: 'MySQLConnector', current_config):
        """
        Method to retrieve the form with a current config
        For example, once the connector is set,
        - we are able to give suggestions for the `database` field
        - if `database` is set, we are able to give suggestions for the `table` field
        """
        constraints = {}

        with suppress(Exception):
            connection = pymysql.connect(
                **connector.get_connection_params(
                    cursorclass=None, database=current_config.get('database')
                )
            )
            # Always add the suggestions for the available databases
            with connection.cursor() as cursor:
                cursor.execute('SHOW DATABASES;')
                res = cursor.fetchall()
                # res = (('information_schema',), ('mysql_db',))
                available_dbs = [db_name for (db_name,) in res]
                constraints['database'] = strlist_to_enum('database', available_dbs)

                if 'database' in current_config:
                    cursor.execute('SHOW TABLES;')
                    res = cursor.fetchall()
                    available_tables = [table_name for (table_name,) in res]
                    constraints['table'] = strlist_to_enum('table', available_tables)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class MySQLConnector(ToucanConnector):
    """
    Import data from MySQL database.
    """

    data_source_model: MySQLDataSource

    host: str = Field(
        ...,
        description='The domain name (preferred option as more dynamic) or '
        'the hardcoded IP address of your database server',
    )
    port: int = Field(None, description='The listening port of your database server')
    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    charset: str = Field(
        'utf8mb4',
        title='Charset',
        description='Character encoding. You should generally let the default "utf8mb4" here.',
    )
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, '
        'i.e. the maximum length of time you want to wait '
        'for the server to respond. None by default',
    )

    def get_connection_params(self, *, database=None, cursorclass=pymysql.cursors.DictCursor):
        conv = pymysql.converters.conversions.copy()
        conv[246] = float
        con_params = {
            'host': self.host,
            'user': self.user,
            'password': self.password.get_secret_value() if self.password else None,
            'port': self.port,
            'database': database,
            'charset': self.charset,
            'connect_timeout': self.connect_timeout,
            'conv': conv,
            'cursorclass': cursorclass,
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    @staticmethod
    def _get_details(index: int, status: Optional[bool]):
        checks = ['Hostname resolved', 'Port opened', 'Host connection', 'Authenticated']
        ok_checks = [(c, True) for i, c in enumerate(checks) if i < index]
        new_check = (checks[index], status)
        not_validated_checks = [(c, None) for i, c in enumerate(checks) if i > index]
        return ok_checks + [new_check] + not_validated_checks

    def get_status(self) -> ConnectorStatus:
        # Check hostname
        try:
            self.check_hostname(self.host)
        except Exception as e:
            return ConnectorStatus(status=False, details=self._get_details(0, False), error=str(e))

        # Check port
        try:
            self.check_port(self.host, self.port)
        except Exception as e:
            return ConnectorStatus(status=False, details=self._get_details(1, False), error=str(e))

        # Check basic access
        try:
            pymysql.connect(**self.get_connection_params())
        except pymysql.err.OperationalError as e:
            error_code = e.args[0]

            # Can't connect to full URI
            if error_code == CR.CR_CONN_HOST_ERROR:
                return ConnectorStatus(
                    status=False, details=self._get_details(2, False), error=e.args[1]
                )

            # Wrong user/password
            if error_code == ER.ACCESS_DENIED_ERROR:
                return ConnectorStatus(
                    status=False, details=self._get_details(3, False), error=e.args[1]
                )

        return ConnectorStatus(status=True, details=self._get_details(3, True), error=None)

    @staticmethod
    def clean_response(response):
        for elt in response:
            for k, v in elt.items():
                if v is None:
                    elt[k] = np.nan
                elif isinstance(v, bytes):
                    elt[k] = v.decode('utf8')
        return response

    @staticmethod
    def execute_and_fetchall(query, connection):
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

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
        tmp_df = df.merge(f_df, left_on=f_key, right_on=f_table_key, how='outer', suffixes=suffixes)

        MySQLConnector.logger.info('Merged two DataFrames...')
        if f_key != f_table_key:
            if f_table_key not in tmp_df.columns:
                MySQLConnector.logger.info(f' and dropped one column {f_table_key}{suffixes[0]}')
                return tmp_df.drop(f_table_key + suffixes[0], axis=1)
            MySQLConnector.logger.info(' no column dropped')
            return tmp_df.drop(f_table_key, axis=1)
        return tmp_df

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
            info['f_key'], idx = MySQLConnector.extract_info_word(
                fetch_all, idx, ['FOREIGN', 'KEY']
            )
            if idx == -1:
                MySQLConnector.logger.info('No (other) foreign key.')
                return res

            info['f_table'], idx = MySQLConnector.extract_info_word(fetch_all, idx, ['REFERENCES'])
            if idx == -1:
                MySQLConnector.logger.error(
                    f"Foreign key {info['f_key']}, found but no REFERENCES found."
                )
                return res

            info['f_table_key'], idx = MySQLConnector.extract_info_word(fetch_all, idx, [])
            if idx == -1:
                MySQLConnector.logger.error(
                    f"Foreign key {info[f'_key']} and REFERENCES found but no foreign table key."
                )
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

    @staticmethod
    def get_foreign_key_info(table_name, connection):
        """
        Get the foreign key information from a table: foreign key inside the
        table, foreign table, key inside that table.
        Args:
            table_name: table name...

        Returns: list of dicts ('f_key', 'f_table', 'f_table_key')

        """
        # TODO: see if there is a more efficient way to do this
        fetch_all_query = f'show create table {table_name}'
        fetch_all_list = MySQLConnector.execute_and_fetchall(fetch_all_query, connection)

        keys = list(fetch_all_list[0].keys())
        if 'Create Table' in keys:
            fetch_all = fetch_all_list[0]['Create Table']
        elif 'Create View' in keys:
            fetch_all = fetch_all_list[0]['Create View']
        else:
            raise InvalidQuery(keys)
        return MySQLConnector.extract_info(fetch_all)

    @staticmethod
    def decode_df(df):
        """
        Used to change bytes columns to string columns
        (can be moved to be applied for all connectors if needed)
        It retrieves all the string columns and converts them all together.
        The string columns become nan columns so we remove them from the result,
        we keep the rest and insert it back to the dataframe
        """
        str_df = df.select_dtypes([np.object])
        if str_df.empty:
            return df

        str_df = str_df.stack().str.decode('utf8').unstack().dropna(axis=1, how='all')
        for col in str_df.columns:
            df[col] = str_df[col]
        return df

    def _retrieve_data(self, datasource):
        """
        Transform a table into a DataFrame and recursively merge tables
        with a foreign key.
        Returns: DataFrames from config['table'].
        """

        connection = pymysql.connect(**self.get_connection_params(database=datasource.database))

        # ----- Prepare -----
        if datasource.query:
            query = datasource.query
            # Extract table name for logging purpose (see below)
            m = re.search(
                r'from\s*(?P<table>[^\s]+)\s*(where|order by|group by|limit)?', query, re.I
            )
            table = m.group('table')
        else:
            table = datasource.table
            query = f'select * from {table}'

        MySQLConnector.logger.debug(f'Executing query : {query}')
        query_params = datasource.parameters or {}

        if not datasource.follow_relations:
            df = pandas_read_sql(query, con=connection, params=query_params)
            df = self.decode_df(df)
            connection.close()
            return df

        # list used because we cannot reassign a variable to update the dataframe.
        # After the merge: append the new DataFrame and remove the pop the first
        # element.
        lres = [pandas_read_sql(query, con=connection, params=query_params)]
        MySQLConnector.logger.info(f'{table} : dumped first DataFrame')

        # ----- Merge -----

        # Avoid circular dependency (unnecessary merges and can add and rename
        # to many columns.
        infos = []
        has_been_merged = {table}
        foreign_keys_append = self.get_foreign_key_info(table, connection)
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
                f"{table} <> found foreign key: {table_info['f_key']} "
                f"inside {table_info['f_table']}"
            )

            f_df = pandas_read_sql(f'select * from {table_info["f_table"]}', con=connection)
            suffixes = ('_' + table, '_' + table_info['f_table'])
            lres.append(
                self._merge_drop(
                    lres[0], f_df, suffixes, table_info['f_key'], table_info['f_table_key']
                )
            )

            if lres:
                del lres[0]

            has_been_merged.add(table_info['f_table'])

            table = table_info['f_table']
            foreign_keys_append = self.get_foreign_key_info(table, connection)
            if len(foreign_keys_append) > 0:
                for keys in foreign_keys_append:
                    infos.append(keys)

        connection.close()

        return lres.pop()


class InvalidQuery(Exception):
    """raised when a query is invalid"""
