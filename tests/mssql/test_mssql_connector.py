import pytest

from connectors.abstract_connector import MissingConnectorName, MissingConnectorOption
from connectors.mssql.mssql_connector import MSSQLConnector
from connectors.sql_connector import UnableToConnectToDatabaseException


def test_missing_server_name():
    """ It should throw a missing connector name error """
    with pytest.raises(MissingConnectorName):
        MSSQLConnector()


def test_missing_dict():
    """ It should throw a missing connector option error """
    with pytest.raises(MissingConnectorOption):
        MSSQLConnector(name='mssql')


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        MSSQLConnector(name='mssql', host='lolcathost', db='blah', user='ubuntu',
                       connect_timeout=1).open_connection()
