import pymssql

from ..sql_connector import SQLConnector, InvalidSQLQuery


class MSSQLConnector(SQLConnector):
    """ A back-end connector to retrieve data from a MSSQL database """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_required_args(self):
        return ['host', 'user']

    def _get_optional_args(self):
        return ['server', 'password', 'port', 'connect_timeout']

    def _get_provider_connection(self):
        self.connection_params['as_dict'] = True
        return pymssql.connect(**self.connection_params)

    def query(self, query, fields=None):
        self.open_connection()
        try:
            return self._df_from_query(query)
        except Exception as e:
            try:
                _, msg = e.args
            except ValueError:
                msg = str(e)
            MSSQLConnector.logger.error(''.join(['query error: ', query, ' <> msg: ', msg]))
            raise InvalidSQLQuery(msg)

    def get_df(self, config):
        """
        Returns: DataFrame from provided query

        """
        self.open_connection()
        query = config['query']
        MSSQLConnector.logger.info(query + ': executing...')
        return self._df_from_query(query.encode('utf8'))
