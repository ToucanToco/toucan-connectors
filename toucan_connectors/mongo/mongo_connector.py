import pandas as pd
import pymongo

from toucan_connectors.abstract_connector import AbstractConnector, MissingQueryParameter


class MongoConnector(AbstractConnector, type='MongoDB'):
    """ A back-end connector to retrieve data from a MongoDB database """

    def __init__(self, *, host, port, database, username=None, password=None):
        if username is not None and password is not None:
            self.uri = f'mongodb://{username}:{password}@{host}:{port}'
        else:
            self.uri = f'mongodb://{host}:{port}'
        self.database = database
        self.client = None

    def connect(self):
        self.client = pymongo.MongoClient(self.uri)

    def disconnect(self):
        self.client.close()

    def _query(self, collection, query):
        """
        Args:
            query (str, dict or list)
            collection (str)
        Returns:
            data (pymongo.cursor.Cursor):
        """
        cursor = self.client[self.database][collection]

        if isinstance(query, str):
            return cursor.find({'domain': query})
        elif isinstance(query, dict):
            return cursor.find(query)
        elif isinstance(query, list):
            return cursor.aggregate(query)

    def _get_df(self, config):
        """
        Args:
            config (dict): The block from ETL config
        Returns:
            df (DataFrame): A dataframe with the response
        """
        if any(s not in config for s in ('collection', 'query')):
            raise MissingQueryParameter('"collection" and "query" are mandatory to get a df')

        data = self.query(config['collection'], config['query'])

        df = pd.DataFrame(list(data))
        return df
