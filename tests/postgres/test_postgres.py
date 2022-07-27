import pandas as pd
import psycopg2
import pytest
from pydantic import ValidationError

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.postgres.postgresql_connector import (
    PostgresConnector,
    PostgresDataSource,
    pgsql,
)

pytestmark = pytest.mark.serial


@pytest.fixture(scope='module')
def postgres_server(service_container):
    def check(host_port):
        conn = psycopg2.connect(
            host='127.0.0.1',
            port=host_port,
            database='postgres_db',
            user='ubuntu',
            password='ilovetoucan',
        )
        cur = conn.cursor()
        cur.execute('SELECT 1;')
        cur.close()
        conn.close()

    return service_container('postgres', check, psycopg2.Error)


@pytest.fixture
def postgres_connector(postgres_server):
    return PostgresConnector(
        name='test',
        host='localhost',
        user='ubuntu',
        password='ilovetoucan',
        default_database='postgres_db',
        port=postgres_server['port'],
    )


def test_get_status_all_good(postgres_connector):
    assert postgres_connector.get_status() == ConnectorStatus(
        status=True,
        details=[
            ('Host resolved', True),
            ('Port opened', True),
            ('Connected to PostgreSQL', True),
            ('Authenticated', True),
            ('Default Database connection', True),
        ],
    )


def test_get_status_bad_host(postgres_connector):
    postgres_connector.host = 'bad_host'
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Host resolved', False),
        ('Port opened', None),
        ('Connected to PostgreSQL', None),
        ('Authenticated', None),
        ('Default Database connection', None),
    ]
    assert status.error == '[Errno -3] Temporary failure in name resolution'


def test_get_status_bad_port(postgres_connector):
    postgres_connector.port = 9999
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Host resolved', True),
        ('Port opened', False),
        ('Connected to PostgreSQL', None),
        ('Authenticated', None),
        ('Default Database connection', None),
    ]
    assert status.error == '[Errno 111] Connection refused'


def test_get_status_bad_connection(postgres_connector, unused_port, mocker):
    postgres_connector.port = unused_port()
    mocker.patch(
        'toucan_connectors.postgres.postgresql_connector.PostgresConnector.check_port',
        return_value=True,
    )
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Host resolved', True),
        ('Port opened', True),
        ('Connected to PostgreSQL', False),
        ('Authenticated', None),
        ('Default Database connection', None),
    ]
    assert 'Connection refused' in status.error


def test_get_status_bad_authentication(postgres_connector):
    postgres_connector.user = 'pika'
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Host resolved', True),
        ('Port opened', True),
        ('Connected to PostgreSQL', True),
        ('Authenticated', False),
        ('Default Database connection', None),
    ]
    assert 'password authentication failed for user "pika"' in status.error


def test_get_status_bad_default_database_connection(postgres_connector):
    postgres_connector.default_database = 'zikzik'
    status = postgres_connector.get_status()
    assert status.status is False
    assert status.details == [
        ('Host resolved', True),
        ('Port opened', True),
        ('Connected to PostgreSQL', True),
        ('Authenticated', True),
        ('Default Database connection', False),
    ]
    assert 'database "zikzik" does not exist' in status.error


def test_no_user():
    """It should raise an error as no user is given"""
    with pytest.raises(ValidationError):
        PostgresConnector(host='some_host', name='test')


def test_open_connection():
    """It should not open a connection"""
    with pytest.raises(psycopg2.OperationalError):
        ds = PostgresDataSource(domain='pika', name='pika', database='circle_test', query='q')
        PostgresConnector(name='test', host='lolcathost', user='ubuntu', connect_timeout=1).get_df(
            ds
        )


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        PostgresDataSource(domaine='test', name='test', database='ubuntu', query='')


def test_datasource():
    with pytest.raises(ValidationError):
        PostgresDataSource(name='mycon', domain='mydomain', database='ubuntu', query='')

    with pytest.raises(ValueError) as exc_info:
        PostgresDataSource(name='mycon', domain='mydomain', database='ubuntu')
    assert "'query' or 'table' must be set" in str(exc_info.value)

    ds = PostgresDataSource(name='mycon', domain='mydomain', database='ubuntu', table='test')
    assert ds.query == 'select * from test;'
    assert ds.language == 'sql'
    assert hasattr(ds, 'query_object')


def test_postgress_get_df(mocker):
    snock = mocker.patch('psycopg2.connect')
    reasq = mocker.patch('pandas.read_sql')

    postgres_connector = PostgresConnector(
        name='test', host='localhost', user='ubuntu', password='ilovetoucan', port=22
    )

    ds = PostgresDataSource(
        domain='test',
        name='test',
        database='postgres_db',
        query='SELECT Name, CountryCode, Population FROM City LIMIT 2;',
    )
    postgres_connector.get_df(ds)

    snock.assert_called_once_with(
        host='localhost', dbname='postgres_db', user='ubuntu', password='ilovetoucan', port=22
    )

    reasq.assert_called_once_with(
        'SELECT Name, CountryCode, Population FROM City LIMIT 2;', con=snock(), params={}
    )


def test_retrieve_response(postgres_connector):
    """It should connect to the database and retrieve the response to the query"""
    ds = PostgresDataSource(
        domain='test',
        name='test',
        database='postgres_db',
        query='SELECT Name, CountryCode, Population FROM City LIMIT 2;',
    )
    res = postgres_connector.get_df(ds)
    assert isinstance(res, pd.DataFrame)
    assert res.shape == (2, 3)


def test_get_df_db(postgres_connector):
    """It should extract the table City and make some merge with some foreign key."""
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM City WHERE Population > %(max_pop)s',
        'parameters': {'max_pop': 5000000},
    }
    expected_columns = {'id', 'name', 'countrycode', 'district', 'population'}
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_db_jinja_syntax(postgres_connector):
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM City WHERE Population > {{ max_pop }}',
        'parameters': {'max_pop': 5000000},
    }
    expected_columns = {'id', 'name', 'countrycode', 'district', 'population'}
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)

    assert not df.empty
    assert set(df.columns) == expected_columns
    assert df.shape == (24, 5)


def test_get_df_forbidden_table_interpolation(postgres_connector):
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM %(tablename)s WHERE Population > 5000000',
        'parameters': {'tablename': 'City'},
    }
    data_source = PostgresDataSource(**data_source_spec)
    with pytest.raises(pd.io.sql.DatabaseError) as e:
        postgres_connector.get_df(data_source)
    assert 'interpolating table name is forbidden' in str(e.value)


def test_get_df_array_interpolation(postgres_connector):
    data_source_spec = {
        'domain': 'Postgres test',
        'type': 'external_database',
        'name': 'Some Postgres provider',
        'database': 'postgres_db',
        'query': 'SELECT * FROM City WHERE id in %(ids)s',
        'parameters': {'ids': [1, 2]},
    }
    data_source = PostgresDataSource(**data_source_spec)
    df = postgres_connector.get_df(data_source)
    assert not df.empty
    assert df.shape == (2, 5)


def test_get_form_empty_query(postgres_connector):
    """It should give suggestions of the databases without changing the rest"""
    current_config = {}
    form = PostgresDataSource.get_form(postgres_connector, current_config)
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['postgres', 'postgres_db'],
    }


def test_get_form_query_with_good_database(postgres_connector, mocker):
    """It should give suggestions of the collections"""
    current_config = {'database': 'postgres_db'}
    form = PostgresDataSource.get_form(postgres_connector, current_config)
    assert form['properties']['database'] == {'$ref': '#/definitions/database'}
    assert form['definitions']['database'] == {
        'title': 'database',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['postgres', 'postgres_db'],
    }
    assert form['properties']['table'] == {'$ref': '#/definitions/table'}
    assert form['definitions']['table'] == {
        'title': 'table',
        'description': 'An enumeration.',
        'type': 'string',
        'enum': ['city', 'country', 'countrylanguage'],
    }
    assert form['required'] == ['domain', 'name', 'database']


def test_get_form_connection_fails(mocker, postgres_connector):
    """It should return a form even if connect fails"""
    mocker.patch.object(pgsql, 'connect').side_effect = IOError
    form = PostgresDataSource.get_form(postgres_connector, current_config={})
    assert 'table' in form['properties']


def test_describe(mocker, postgres_connector):
    """It should return a table description"""
    ds = PostgresDataSource(
        domain='test', name='test', database='postgres_db', query='SELECT * FROM city;'
    )
    res = postgres_connector.describe(ds)
    assert res == {
        'id': 'int4',
        'name': 'text',
        'countrycode': 'bpchar',
        'district': 'text',
        'population': 'int4',
    }


def test_describe_error(mocker, postgres_connector):
    """It should return a table description"""
    ds = PostgresDataSource(
        domain='test', name='test', database='postgres_db', query='SELECT * FROM city;'
    )
    mocked_connect = mocker.MagicMock()
    mocked_cursor = mocker.MagicMock()
    mocked_connect.cursor.return_value = mocked_cursor

    def execute(q):
        raise psycopg2.ProgrammingError

    mocked_cursor.__enter__().execute = execute
    mocker.patch(
        'toucan_connectors.postgres.postgresql_connector.pgsql.connect', return_value=mocked_connect
    )

    with pytest.raises(psycopg2.ProgrammingError):
        postgres_connector.describe(ds)


def test_get_model(postgres_connector):
    """Check that it returns the db tree structure"""
    assert postgres_connector.get_model() == [
        {
            'name': 'city',
            'schema': 'public',
            'database': 'postgres_db',
            'type': 'table',
            'columns': [
                {'name': 'id', 'type': 'integer'},
                {'name': 'name', 'type': 'text'},
                {'name': 'countrycode', 'type': 'character'},
                {'name': 'district', 'type': 'text'},
                {'name': 'population', 'type': 'integer'},
            ],
        },
        {
            'name': 'country',
            'schema': 'public',
            'database': 'postgres_db',
            'type': 'table',
            'columns': [
                {'name': 'code', 'type': 'character'},
                {'name': 'name', 'type': 'text'},
                {'name': 'continent', 'type': 'text'},
                {'name': 'region', 'type': 'text'},
                {'name': 'surfacearea', 'type': 'real'},
                {'name': 'indepyear', 'type': 'smallint'},
                {'name': 'population', 'type': 'integer'},
                {'name': 'lifeexpectancy', 'type': 'real'},
                {'name': 'gnp', 'type': 'numeric'},
                {'name': 'gnpold', 'type': 'numeric'},
                {'name': 'localname', 'type': 'text'},
                {'name': 'governmentform', 'type': 'text'},
                {'name': 'headofstate', 'type': 'text'},
                {'name': 'capital', 'type': 'integer'},
                {'name': 'code2', 'type': 'character'},
            ],
        },
        {
            'name': 'countrylanguage',
            'schema': 'public',
            'database': 'postgres_db',
            'type': 'table',
            'columns': [
                {'name': 'countrycode', 'type': 'character'},
                {'name': 'language', 'type': 'text'},
                {'name': 'isofficial', 'type': 'boolean'},
                {'name': 'percentage', 'type': 'real'},
            ],
        },
    ]


def test_raised_error_for_get_model(mocker, postgres_connector):
    """Check that it returns the db tree structure"""
    with mocker.patch.object(
        PostgresConnector, '_list_tables_info', side_effect=psycopg2.OperationalError()
    ):
        assert postgres_connector.get_model() == []


def test_get_model_with_info(postgres_connector):
    """Check that it returns the db tree structure"""
    assert postgres_connector.get_model_with_info() == (
        [
            {
                'name': 'city',
                'schema': 'public',
                'database': 'postgres_db',
                'type': 'table',
                'columns': [
                    {'name': 'id', 'type': 'integer'},
                    {'name': 'name', 'type': 'text'},
                    {'name': 'countrycode', 'type': 'character'},
                    {'name': 'district', 'type': 'text'},
                    {'name': 'population', 'type': 'integer'},
                ],
            },
            {
                'name': 'country',
                'schema': 'public',
                'database': 'postgres_db',
                'type': 'table',
                'columns': [
                    {'name': 'code', 'type': 'character'},
                    {'name': 'name', 'type': 'text'},
                    {'name': 'continent', 'type': 'text'},
                    {'name': 'region', 'type': 'text'},
                    {'name': 'surfacearea', 'type': 'real'},
                    {'name': 'indepyear', 'type': 'smallint'},
                    {'name': 'population', 'type': 'integer'},
                    {'name': 'lifeexpectancy', 'type': 'real'},
                    {'name': 'gnp', 'type': 'numeric'},
                    {'name': 'gnpold', 'type': 'numeric'},
                    {'name': 'localname', 'type': 'text'},
                    {'name': 'governmentform', 'type': 'text'},
                    {'name': 'headofstate', 'type': 'text'},
                    {'name': 'capital', 'type': 'integer'},
                    {'name': 'code2', 'type': 'character'},
                ],
            },
            {
                'name': 'countrylanguage',
                'schema': 'public',
                'database': 'postgres_db',
                'type': 'table',
                'columns': [
                    {'name': 'countrycode', 'type': 'character'},
                    {'name': 'language', 'type': 'text'},
                    {'name': 'isofficial', 'type': 'boolean'},
                    {'name': 'percentage', 'type': 'real'},
                ],
            },
        ],
        {},
    )


def test_raised_error_for_get_model_with_info(mocker, postgres_connector):
    """Check that it returns the db tree structure"""
    with mocker.patch.object(
        PostgresConnector, '_list_tables_info', side_effect=psycopg2.OperationalError
    ):
        assert postgres_connector.get_model_with_info() == (
            [],
            {'info': {'Could not reach databases': ['postgres', 'postgres_db']}},
        )
