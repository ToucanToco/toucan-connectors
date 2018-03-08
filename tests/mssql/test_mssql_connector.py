import pytest

from connectors.abstract_connector import BadParameters, UnableToConnectToDatabaseException
from connectors.mssql.mssql_connector import MSSQLConnector


def test_missing_server_name():
    """ It should throw a missing connector name error """
    with pytest.raises(BadParameters):
        MSSQLConnector()


def test_open_connection():
    """ It should not open a connection """
    with pytest.raises(UnableToConnectToDatabaseException):
        MSSQLConnector(host='lolcathost', db='blah', user='ubuntu', connect_timeout=1).__enter__()
