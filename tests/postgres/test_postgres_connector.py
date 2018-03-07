import psycopg2
import pytest

from connectors.postgres import PostgresConnector


@pytest.fixture(scope='module')
def postgres_server(service_container):
    def check(host_port):
        conn = psycopg2.connect(host='127.0.0.1', port=host_port, database='postgres_db',
                                user='ubuntu', password='ilovetoucan')
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        cur.close()
        conn.close()

    return service_container('mysql', check, psycopg2.Error)


@pytest.fixture()
def connector(postgres_server):
    return PostgresConnector(name='mysql', host='localhost', db='mysql_db',
                             user='ubuntu', password='ilovetoucan', port=postgres_server['port'])


    # class PostgresConnectorTestCase(TestCase):
    #     def test_available(self):
    #         self.assertIn('Postgres', AVAILABLE_CONNECTORS)
    #
    #     def test_required_args(self):
    #         with self.assertRaises(MissingConnectorOption):
    #             PostgresConnector(name='a_connector_has_no_name',
    #                               host='some_host',
    #                               bla='missing_something')
    #
    #         connector = PostgresConnector(name='a_connector_has_no_name',
    #                                       host='localhost',
    #                                       db='circle_test',
    #                                       user='ubuntu',
    #                                       bla='missing_something')
    #         self.assertTrue(all(
    #             [arg in connector.connection_params
    #              for arg in connector._get_required_args()]
    #         ))
    #         self.assertNotIn('bla', connector.connection_params)
    #
    #     def test_normalized_args(self):
    #         connector = PostgresConnector(name='a_connnector_does_have_a_name',
    #                                       host='some_host',
    #                                       user='DennisRitchie',
    #                                       bla='missing_something',
    #                                       db='some_db')
    #
    #         chargs = connector._changes_normalize_args()
    #         self.assertTrue(
    #             all(change in connector.connection_params for change in list(chargs.values())))
    #         self.assertTrue('db' not in connector.connection_params)
    #
    #     def test_open_connection(self):
    #         """
    #         It should not open a connection
    #         """
    #         with self.assertRaises(UnableToConnectToDatabaseException):
    #             PostgresConnector(name='pgsql',
    #                               host='lolcathost',
    #                               db='circle_test',
    #                               user='ubuntu',
    #                               connect_timeout=1).open_connection()
    #
    #     @staticmethod
    #     def instanciate_connector():
    #         return PostgresConnector(name='pgsql', host='localhost', db='circle_test', user='ubuntu')
    #
    #     def test_retrieve_response(self):
    #         """
    #         It should connect to the database and retrieve the response to the query
    #         """
    #         connector = self.instanciate_connector()
    #         with self.assertRaises(InvalidSQLQuery):
    #             connector.query("")
    #         res = connector.query("SELECT Name, CountryCode, Population FROM City LIMIT 2;")
    #         self.assertIsInstance(res, list)
    #         self.assertIsInstance(res[0], tuple)
    #         self.assertEqual(len(res[0]), 3)
    #
    #     @patch('pandas.read_sql')
    #     def test_get_df(self, mock_read_sql):
    #         """
    #         It should call the sql extractor
    #         """
    #         connector = self.instanciate_connector()
    #         mock_read_sql.return_value = pd.DataFrame.from_dict({
    #             'a': [1, 2],
    #             'b': [3, 4]
    #         })
    #         data_sources_spec = [
    #             {
    #                 'domain': "Postgres test",
    #                 'type': "external_database",
    #                 'name': "Some MySQL provider",
    #                 'query': 'SELECT * FROM city;'
    #             }
    #         ]
    #
    #         df = connector.get_df(data_sources_spec[0])
    #         self.assertEqual(df.shape, (2, 2))
    #
    #     def test_get_df_db(self):
    #         """
    #         It should extract the table City and make some merge with some foreign
    #         key.
    #         """
    #         connector = self.instanciate_connector()
    #         data_sources_spec = {
    #             'domain': "Postgres test",
    #             'type': "external_database",
    #             'name': "Some Postgres provider",
    #             'query': 'SELECT * FROM city;'
    #         }
    #
    #         expected_columns = ['id', 'name', 'countrycode', 'district', 'population']
    #
    #         df = connector.get_df(data_sources_spec)
    #
    #         self.assertFalse(df.empty)
    #         self.assertTrue(len(df.columns) == len(expected_columns),
    #                         f'{len(df)} columns instead of {len(expected_columns)}')
    #
    #         self.assertTrue(len(df[df['population'] > 5000000]) == 24,
    #                         'Should find 24 cities with more than 5 000 000 people')
