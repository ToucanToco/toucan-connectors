import os

import cx_Oracle
import pytest

from toucan_connectors.oracle import OracleConnector


@pytest.fixture(scope='module')
def oracle_server(service_container):
    def check(host_port):
        conn = cx_Oracle.connect(user='sys', password='ilovetoucan',
                                 dsn=f'127.0.0.1:{host_port}/oracle_db')
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")

        sql_query_path = f'{os.path.dirname(__file__)}/fixtures/world.sql'
        with open(sql_query_path) as f:
            sql_query = f.read()
        cursor.execute(sql_query)
        conn.commit()

        cursor.close()
        conn.close()

    return service_container('oracle', check, cx_Oracle.Error)


@pytest.fixture()
def oracle_connector(oracle_server):
    return OracleConnector(host='127.0.0.1', user='sys',
                           password='ilovetoucan', db='oracle_db')


def test_get_df_db(oracle_connector):
    """" It should extract the table City and make some merge with some foreign key """
    data_sources_spec = [
        {
            'domain': 'Oracle test',
            'type': 'external_database',
            'name': 'Some Oracle provider',
            'table': 'City'
        }
    ]

    expected_columns = ['ID', 'Name_City', 'CountryCode', 'District',
                        'Population_City', 'Name_Country', 'Continent',
                        'Region', 'SurfaceArea', 'IndepYear',
                        'Population_Country', 'LifeExpectancy', 'GNP',
                        'GNPOld', 'LocalName', 'GovernmentForm', 'HeadOfState',
                        'Capital', 'Code2']

    df = oracle_connector.get_df(data_sources_spec[0])

    assert not df.empty
    assert len(df.columns) == 19

    assert collections.Counter(df.columns) == collections.Counter(expected_columns)
    assert len(df.columns) == len(expected_columns)

    assert len(df[df['Population_City'] > 5000000]) == 24