import cx_Oracle
import pytest

from toucan_connectors.oracle_sql.oracle_sql_connector import (
    OracleSQLConnector, OracleSQLDataSource
)

missing_oracle_lib = False
try:
    cx_Oracle.connect()
except cx_Oracle.DatabaseError as e:
    missing_oracle_lib = 'DPI-1047' in str(e)


@pytest.fixture(scope='module')
def oracle_server(service_container):
    def check(host_port):
        conn = cx_Oracle.connect(user='system', password='oracle', dsn=f'localhost:{host_port}/xe')
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM City")
        cursor.close()
        conn.close()

    # timeout is set to 5 min as the container takes a very long time to start
    return service_container('oraclesql', check, cx_Oracle.Error, timeout=300)


@pytest.fixture
def oracle_connector(oracle_server):
    return OracleSQLConnector(name='my_oracle_sql_con', user='system', password='oracle',
                              dsn=f'localhost:{oracle_server["port"]}/xe')


def test_oracle_get_df(mocker):
    snock = mocker.patch('cx_Oracle.connect')
    reasq = mocker.patch('pandas.read_sql')

    oracle_connector = OracleSQLConnector(name='my_oracle_sql_con', user='system',
                                          password='oracle', dsn=f'localhost:22/xe')
    datasource = OracleSQLDataSource(domain='Oracle test', name='my_oracle_sql_con',
                                     query='SELECT * FROM City;')
    oracle_connector.get_df(datasource)

    snock.assert_called_once_with(
        user='system',
        password='oracle',
        dsn=f'localhost:22/xe'
    )

    reasq.assert_called_once_with(
        'SELECT * FROM City',
        con=snock()
    )


@pytest.mark.skip(reason="Unavailable image")
def test_get_df_db(oracle_connector):
    """" It should extract the table City and make some merge with some foreign key """
    data_sources_spec = [
        {
            'domain': 'Oracle test',
            'type': 'external_database',
            'name': 'my_oracle_sql_con',
            'query': 'SELECT * FROM City;'
        }
    ]

    expected_columns = ['ID', 'NAME', 'COUNTRYCODE', 'DISTRICT', 'POPULATION']

    data_source = OracleSQLDataSource(**data_sources_spec[0])
    df = oracle_connector.get_df(data_source)

    assert not df.empty
    assert df.shape == (50, 5)
    assert df.columns.tolist() == expected_columns

    assert len(df[df['POPULATION'] > 500000]) == 5
