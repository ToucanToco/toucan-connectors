import pyodbc
import pytest
from pydantic import ValidationError
from pytest_mock import MockFixture

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.databricks.databricks_connector import (
    DataBricksConnectionError,
    DatabricksConnector,
    DatabricksDataSource,
)

CONNECTION_STRING = (
    'Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so;'
    'Host=127.0.0.1;'
    'Port=443;'
    'HTTPPath=foo/path;'
    'ThriftTransport=2;'
    'SSL=1;'
    'AuthMech=3;'
    'UID=token;'
    'PWD=12345'
)

CONNECTION_STATUS_OK = ConnectorStatus(
    status=True,
    error=None,
    details=[
        ('Host resolved', True),
        ('Port opened', True),
        ('Connected to Databricks', True),
        ('Authenticated', True),
    ],
)


def test_postgres_driver_installed():
    """
    Check that pgodbc is installed
    """
    assert 'Simba Spark ODBC Driver' in pyodbc.drivers()


@pytest.fixture
def databricks_connector() -> DatabricksConnector:
    return DatabricksConnector(
        name='test', Host='127.0.0.1', Port='443', HTTPPath='foo/path', PWD='12345'
    )


def test__build_connection_string(databricks_connector: DatabricksConnector):
    assert databricks_connector._build_connection_string() == CONNECTION_STRING


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        DatabricksDataSource(domain='test', name='test', query='')


def test_databricks_get_df(mocker: MockFixture, databricks_connector: DatabricksConnector):
    mock_pyodbc_connect = mocker.patch('pyodbc.connect')
    mock_pandas_read_sql = mocker.patch('pandas.read_sql')
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.get_status',
        return_value=ConnectorStatus(
            status=True,
            error=None,
            details=[
                ('Host resolved', True),
                ('Port opened', True),
                ('Connected to Databricks', True),
                ('Authenticated', True),
            ],
        ),
    )

    ds = DatabricksDataSource(
        domain='test',
        name='test',
        query='SELECT Name, CountryCode, Population from city LIMIT 2;',
    )
    databricks_connector.get_df(ds)
    mock_pyodbc_connect.assert_called_once_with(CONNECTION_STRING, autocommit=True, ansi=False)
    mock_pandas_read_sql.assert_called_once_with(
        'SELECT Name, CountryCode, Population from city LIMIT 2;',
        con=mock_pyodbc_connect(),
        params=[],
    )


def test_query_variability(mocker: MockFixture, databricks_connector: DatabricksConnector):
    """It should connect to the database and retrieve the response to the query"""
    mock_pyodbc_connect = mocker.patch('pyodbc.connect')
    mock_pandas_read_sql = mocker.patch('pandas.read_sql')
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.get_status',
        return_value=ConnectorStatus(
            status=True,
            error=None,
            details=[
                ('Host resolved', True),
                ('Port opened', True),
                ('Connected to Databricks', True),
                ('Authenticated', True),
            ],
        ),
    )

    ds = DatabricksDataSource(
        query='select * from test where id_nb > %(id_nb)s and price > %(price)s;',
        domain='test',
        name='test',
        parameters={'price': 10, 'id_nb': 1},
    )

    databricks_connector.get_df(ds)

    mock_pandas_read_sql.assert_called_once_with(
        'select * from test where id_nb > ? and price > ?;',
        con=mock_pyodbc_connect(),
        params=[1, 10],
    )


def test__retrieve_data_error(databricks_connector: DatabricksConnector) -> None:
    with pytest.raises(DataBricksConnectionError):
        databricks_connector._retrieve_data(
            DatabricksDataSource(
                domain='test',
                name='test',
                query='SELECT Name, CountryCode, Population from city LIMIT 2;',
            )
        )


def test_get_status_all(databricks_connector: DatabricksConnector, mocker: MockFixture):
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname',
        side_effect=Exception('host unavailable'),
    )
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error='host unavailable',
        details=[
            ('Host resolved', False),
            ('Port opened', None),
            ('Connected to Databricks', None),
            ('Authenticated', None),
        ],
    )
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port',
        side_effect=Exception('port closed'),
    )
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname'
    )
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error='port closed',
        details=[
            ('Host resolved', True),
            ('Port opened', False),
            ('Connected to Databricks', None),
            ('Authenticated', None),
        ],
    )
    mocker.patch(
        'pyodbc.connect',
        side_effect=pyodbc.InterfaceError('Authentication/authorization error occured'),
    )
    mocker.patch('toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port')
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname'
    )
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error='Authentication/authorization error occured',
        details=[
            ('Host resolved', True),
            ('Port opened', True),
            ('Connected to Databricks', True),
            ('Authenticated', False),
        ],
    )
    mocker.patch(
        'pyodbc.connect',
        side_effect=pyodbc.InterfaceError("I don't know mate"),
    )
    mocker.patch('toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port')
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname'
    )
    assert databricks_connector.get_status() == ConnectorStatus(
        status=False,
        message=None,
        error="I don't know mate",
        details=[
            ('Host resolved', True),
            ('Port opened', True),
            ('Connected to Databricks', False),
            ('Authenticated', None),
        ],
    )
    mocker.patch('pyodbc.connect')
    mocker.patch('toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_port')
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.check_hostname'
    )
    assert databricks_connector.get_status() == CONNECTION_STATUS_OK


def test__cluster_started_failed(
    mocker: MockFixture, databricks_connector: DatabricksConnector
) -> None:
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.get_status',
        return_value=CONNECTION_STATUS_OK,
    )
    mocker.patch('toucan_connectors.databricks.databricks_connector.sleep')
    connection = mocker.MagicMock()
    fake_cursor = mocker.MagicMock()
    connection.cursor.return_value = fake_cursor

    def raise_error() -> None:
        raise Exception

    fake_cursor.execute = raise_error
    mocker.patch('pyodbc.connect', return_value=connection)
    assert databricks_connector._cluster_started() == (False, None)


def test__cluster_started_error(
    mocker: MockFixture, databricks_connector: DatabricksConnector
) -> None:
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.get_status',
        return_value=CONNECTION_STATUS_OK,
    )
    mocker.patch('pyodbc.connect', side_effect=Exception)
    with pytest.raises(DataBricksConnectionError):
        databricks_connector._cluster_started()


def test__cluster_started_ok(
    mocker: MockFixture, databricks_connector: DatabricksConnector
) -> None:
    mocker.patch(
        'toucan_connectors.databricks.databricks_connector.DatabricksConnector.get_status',
        return_value=CONNECTION_STATUS_OK,
    )
    mocker.patch('toucan_connectors.databricks.databricks_connector.sleep')
    connection = mocker.MagicMock()
    fake_cursor1 = mocker.MagicMock()
    fake_cursor2 = mocker.MagicMock()
    connection.cursor.side_effect = (fake_cursor1, fake_cursor2)

    def raise_error() -> None:
        raise Exception

    fake_cursor1.execute = raise_error
    mocker.patch('pyodbc.connect', return_value=connection)
    assert databricks_connector._cluster_started() == (True, connection)
