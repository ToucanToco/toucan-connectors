import socket

import pandas as pd
import pytest

from toucan_connectors.odata.odata_connector import ODataConnector, ODataDataSource


def test_get_df():
    """
    It should make a query to the canonical service and return the right results
    """
    expected_df = pd.read_json('tests/odata/fixtures/records.json', orient='records')

    provider = ODataConnector(
        name='test',
        url='http://services.odata.org/V4/Northwind/Northwind.svc/')

    data_source = ODataDataSource(
        domain='test',
        name='test',
        entity='Orders',
        query={"$filter": "ShipCountry eq 'France'",
               "$orderby": "Freight desc",
               "$skip": 50,
               "$top": 3})

    try:
        df = provider.get_df(data_source)
        sl = ['CustomerID', 'EmployeeID', 'Freight']
        assert df[sl].equals(expected_df[sl])
    except socket.error:
        pytest.skip('Could not connect to the standard example OData service.')
