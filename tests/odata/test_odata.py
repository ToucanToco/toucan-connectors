import pandas as pd
import pytest
from odata.metadata import MetaData

from toucan_connectors.odata.odata_connector import ODataConnector, ODataDataSource


@pytest.mark.skip(reason="This tests makes a request to an API that returns a 500")
def test_get_df(mocker):
    """
    It should make a query to the canonical service and return the right results
    """
    spy_load_metadata = mocker.spy(MetaData, "load_document")
    expected_df = pd.read_json("tests/odata/fixtures/records.json", orient="records")

    provider = ODataConnector(
        name="test",
        baseroute="http://services.odata.org/V4/Northwind/Northwind.svc/",
        auth={"type": "basic", "args": ["u", "p"]},
    )

    data_source = ODataDataSource(
        domain="test",
        name="test",
        entity="Orders",
        query={
            "$filter": "ShipCountry eq 'France'",
            "$orderby": "Freight desc",
            "$skip": 50,
            "$top": 3,
        },
    )

    try:
        df = provider.get_df(data_source)
        sl = ["CustomerID", "EmployeeID", "Freight"]
        assert df[sl].equals(expected_df[sl])
    except OSError:
        pytest.skip("Could not connect to the standard example OData service.")

    assert spy_load_metadata.call_count == 1
    args, _ = spy_load_metadata.call_args
    assert args[0].url.endswith("/$metadata")

    provider.auth = None
    try:
        provider.get_df(data_source)
    except OSError:
        pytest.skip("Could not connect to the standard example OData service.")
