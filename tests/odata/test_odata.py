import pandas as pd

from toucan_connectors.odata.odata_connector import ODataConnector, ODataDataSource

def test_get_df():
    expected_df = pd.DataFrame([
        {'CustomerID': 'FOLIG',
         'EmployeeID': 8,
         'Freight': 11.26,
         'OrderDate': '1997-01-08T00:00:00Z',
         'OrderID': 10408,
         'RequiredDate': '1997-02-05T00:00:00Z',
         'ShipAddress': '184, chaussée de Tournai',
         'ShipCity': 'Lille',
         'ShipCountry': 'France',
         'ShipName': 'Folies gourmandes',
         'ShipPostalCode': '59000',
         'ShipRegion': None,
         'ShipVia': 1,
         'ShippedDate': '1997-01-14T00:00:00Z'},
        {'CustomerID': 'VINET',
         'EmployeeID': 3,
         'Freight': 11.08,
         'OrderDate': '1997-11-12T00:00:00Z',
         'OrderID': 10739,
         'RequiredDate': '1997-12-10T00:00:00Z',
         'ShipAddress': "59 rue de l'Abbaye",
         'ShipCity': 'Reims',
         'ShipCountry': 'France',
         'ShipName': 'Vins et alcools Chevalier',
         'ShipPostalCode': '51100',
         'ShipRegion': None,
         'ShipVia': 3,
         'ShippedDate': '1997-11-17T00:00:00Z'},
        {'CustomerID': 'BONAP',
         'EmployeeID': 1,
         'Freight': 11.06,
         'OrderDate': '1997-05-02T00:00:00Z',
         'OrderID': 10525,
         'RequiredDate': '1997-05-30T00:00:00Z',
         'ShipAddress': '12, rue des Bouchers',
         'ShipCity': 'Marseille',
         'ShipCountry': 'France',
         'ShipName': "Bon app'",
         'ShipPostalCode': '13008',
         'ShipRegion': None,
         'ShipVia': 2,
         'ShippedDate': '1997-05-23T00:00:00Z'}
    ])

    odata_connector = ODataConnector(
        name='test',
        url='http://services.odata.org/V4/Northwind/Northwind.svc/',
        username='xxx',
        password='xxx')
    data_source = ODataDataSource(
        domain='test',
        name='test',
        entity='Orders',
        query={"$filter": "ShipCountry eq 'France'",
               "$orderby": "Freight desc",
               "$skip": 50,
               "$top": 3})
    df = odata_connector.get_df(data_source)

    assert df.equals(expected_df)
