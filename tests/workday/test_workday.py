import pytest
import workday.exceptions

from toucan_connectors.workday.workday_connector import WorkdayConnector, WorkdayDataSource


@pytest.fixture(scope='function')
def connector():
    return WorkdayConnector(
        name='myWorkdayConnector',
        type='Workday',
        tenant='umanis',
        username='<username>',
        password='<password>',
    )


@pytest.fixture(scope='function')
def data_source():
    return WorkdayDataSource(
        name='myWorkdayDataSource',
        domain='Workers',
        service='Human_Resources',
        service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2',
        operation='Get_Workers',
        request_parameters={
            'Request_References': {
                'Worker_Reference': [
                    {'ID': {'_value_1': '9836946ba6f401b18fe9c3a98d21ead6', 'type': 'WID'}},
                    {'ID': {'_value_1': '9836946ba6f40107d53788a88d2174d4', 'type': 'WID'}},
                ]
            }
        },
        filter='[.Worker[].Worker_Data | {User_ID: .User_ID, Worker_ID: .Worker_ID}]',
    )


def test_get_df_Get_Workers(connector, data_source):
    df = connector.get_df(data_source)
    assert df.shape == (2, 2)


def test_get_df_Get_Absences(connector):
    data_source_absence = WorkdayDataSource(
        name='myWorkdayDataSource',
        domain='Absence',
        service='Absence_Management',
        service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Absence_Management/v33.2',
        operation='Get_Absence_Inputs',
        request_parameters={
            'Request_References': {
                'Absence_Input_Reference': [
                    {'ID': {'_value_1': '08d4a6121b760154a3c7d6e1600c6646', 'type': 'WID'}},
                    {'ID': {'_value_1': '08d4a6121b76019c685b242a610cea54', 'type': 'WID'}},
                ]
            }
        },
        filter='[.Absence_Input[].Absence_Input_Data | {Start_Date: .Start_Date, End_Date: .End_Date, Batch_ID: .Batch_ID}]',
    )
    df = connector.get_df(data_source_absence)
    assert df.shape == (2, 3)


def test_get_df_Get_Locations(connector):
    data_source_absence = WorkdayDataSource(
        name='myWorkdayDataSource',
        domain='Referentiel',
        service='Human_Resources',
        service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2',
        operation='Get_Locations',
        request_parameters={
            'Request_References': {
                'Location_Reference': [
                    {'ID': {'_value_1': 'b4ee7b16974c01f764138b598d01915c', 'type': 'WID'}}
                ]
            }
        },
        filter='[.Location[].Location_Data | {Location_ID: .Location_ID, Location_Name: .Location_Name}]',
    )
    df = connector.get_df(data_source_absence)
    assert df.shape == (1, 2)


def test_exceptions_wrong_password(data_source):
    connectorWrong = WorkdayConnector(
        name='myWorkdayConnector',
        type='Workday',
        tenant='umanis',
        username='<username>',
        password='WrongPassword',
    )

    with pytest.raises(workday.exceptions.WorkdaySoapApiError):
        connectorWrong.get_df(data_source)


def test_exceptions_wrong_username(data_source):
    connectorWrong = WorkdayConnector(
        name='myWorkdayConnector',
        type='Workday',
        tenant='umanis',
        username='WrongUsername',
        password='<password>',
    )

    with pytest.raises(workday.exceptions.WorkdaySoapApiError):
        connectorWrong.get_df(data_source)


def test_exceptions_wrong_tenant(data_source):
    connectorWrong = WorkdayConnector(
        name='myWorkdayConnector',
        type='Workday',
        tenant='WrongTenant',
        username='<username>',
        password='<password>',
    )

    with pytest.raises(workday.exceptions.WorkdaySoapApiError):
        connectorWrong.get_df(data_source)


def test_exceptions_wrong_WSDL(connector):
    data_sourceWrong = WorkdayDataSource(
        name='myWorkdayDataSource',
        domain='Workers',
        service='Human_Resources',
        service_WSDL_URL='/',
        operation='Get_Workers',
        request_parameters={
            'Request_References': {
                'Worker_Reference': {
                    'ID': {'_value_1': '9836946ba6f401b18fe9c3a98d21ead6', 'type': 'WID'}
                }
            }
        },
        filter='[.Worker[].Worker_Data | {User_ID: .User_ID, Worker_ID: .Worker_ID}]',
    )

    with pytest.raises(FileNotFoundError):
        connector.get_df(data_sourceWrong)


def test_exceptions_wrong_operation(connector):
    data_sourceWrong = WorkdayDataSource(
        name='myWorkdayDataSource',
        domain='Workers',
        service='Human_Resources',
        service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2',
        operation='WrongOperation',
        request_parameters={
            'Request_References': {
                'Worker_Reference': {
                    'ID': {'_value_1': '9836946ba6f401b18fe9c3a98d21ead6', 'type': 'WID'}
                }
            }
        },
        filter='[.Worker[].Worker_Data | {User_ID: .User_ID, Worker_ID: .Worker_ID}]',
    )

    with pytest.raises(AttributeError):
        connector.get_df(data_sourceWrong)


def test_exceptions_wrong_query(connector):
    data_sourceWrong = WorkdayDataSource(
        name='myWorkdayDataSource',
        domain='Workers',
        service='Human_Resources',
        service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2',
        operation='Get_Workers',
        request_parameters={'Request_References': {'WrongQuery': 'WrongParam'}},
        filter='[.Worker[].Worker_Data | {User_ID: .User_ID, Worker_ID: .Worker_ID}]',
    )

    with pytest.raises(TypeError):
        connector.get_df(data_sourceWrong)


@pytest.mark.skip(
    reason="This test is not pertinent, as the 'Service' name is an arbitrary string, just a key to store the WSDL URL in a dict"
)
def test_exceptions_wrong_service(connector):
    data_sourceWrong = WorkdayDataSource(
        name='myWorkdayDataSource',
        domain='Workers',
        service='bla',
        service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2',
        operation='Get_Workers',
        request_parameters={
            'Request_References': {
                'Worker_Reference': {
                    'ID': {'_value_1': '9836946ba6f401b18fe9c3a98d21ead6', 'type': 'WID'}
                }
            }
        },
        filter='[.Worker[].Worker_Data | {User_ID: .User_ID, Worker_ID: .Worker_ID}]',
    )
    print(connector.get_df(data_sourceWrong))
    with pytest.raises(workday.exceptions.WsdlNotProvidedError):
        connector.get_df(data_sourceWrong)
