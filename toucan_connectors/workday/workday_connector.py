import pandas as pd
import workday
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import FilterSchema, transform_with_jq
from pydantic import Field

""" Only supports Anonymous or Credentials authentification for the moment """
""" TODO: implement WS-Security X509-only signed credentials (Recommended by Workday) """
from workday.auth import WsSecurityCredentialAuthentication
from workday.auth import AnonymousAuthentication

from datetime import datetime, timedelta, date
import json
import zeep
import decimal
import types


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return (str(obj) for obj in [obj])
    if isinstance(obj, types.GeneratorType):
        return list(obj)
    raise TypeError ("Type %s not serializable" % type(obj))

def use_jq_to_parse(data, jq_filter):
    try:
        return transform_with_jq(data, jq_filter)
    except ValueError:
        WorkdayConnector.logger.error(f'Could not transform {data} using {jq_filter}')
        raise



class WorkdayDataSource(ToucanDataSource):
    service: str = Field(
        None,
        title='Name of the Workday API Service you want to use.',
        description='Check "Public Web Services" page in Workday',
        example='Human_Resources'
    )
    service_WSDL_URL: str = Field(
        None,
        title='URL of the WSDL of the Workday API Service you want to use.',
        description='https://{Workday domain}/ccx/service/{tenant}/{service}/{version}?wsdl',
        example='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2?wsdl'
    )
    operation: str = Field(
        None,
        title='Name of the Workday API Operation you want to call within the selected Service.',
        description='Check https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html',
        example='Get_Workers'
    )
    request_references_param: dict = Field(
        None,
        title='Request_References Parameter',
        description='Check https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html',
        example="{'Absence_Input_Reference' : {'ID' : {'_value_1': '08d4a6121b760154a3c7d6e1600c6646','type': 'WID'}}}"
    )
    request_criteria_param: dict = Field(
        None,
        title='Request Criteria Parameter',
        description='Check https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html',
        example="{'Exclude_Inactive_Workers' : False}"
    )
    response_filter_param: dict = Field(
        {},
        title='Response Filter Parameter',
        description='Check https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html',
        example="{'As_Of_Effective_Date': effective_date,'As_Of_Entry_DateTime': effective_date}"
    )
    response_group_param: dict = Field(
        None,
        title='Response Group Parameter',
        description='Check https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html',
        example="{'Include_Reference' : True,'Include_Absence_Input_Data' : True}"
    )   
    filter: str = FilterSchema


class WorkdayConnector(ToucanConnector):
    data_source_model: WorkdayDataSource

    tenant: str = Field(
        None,
        title='Name of the Workday tenant',
        description='Name of the Workday tenant',
        example='umanis'
    )
    username: str = Field(
        None,
        title='Username for authentification',
        description='Username for Credential authentification. Leave empty for Anonymous authentification.',
        example='pcadoret'
    )
    password: str = Field(
        None,
        title='Password for authentification',
        description='Password for Credential authentification',
        example='$eCre7P4s$w0rd!'
    )




    def _retrieve_data(self, data_source: WorkdayDataSource) -> pd.DataFrame:
        """ Only supports Anonymous or Credentials authentification """

        if self.username:
            auth = WsSecurityCredentialAuthentication('@'.join((self.username, self.tenant)), self.password)
        else:
            auth = AnonymousAuthentication()

        apis = {
            data_source.service: data_source.service_WSDL_URL
        }

        client = workday.WorkdayClient(
            wsdls=apis,
            authentication=auth
        )
        print(client)


        data = getattr(getattr(client, data_source.service) , data_source.operation)(    
            Request_References = data_source.request_references_param,
            Request_Criteria = data_source.request_criteria_param,
            Response_Filter = data_source.response_filter_param,
            Response_Group = data_source.response_group_param,
        )
        
        num_page = data.total_pages

        data_json = json.loads(json.dumps(zeep.helpers.serialize_object(data.data), default=json_serial))

        df = pd.DataFrame(use_jq_to_parse(data_json, data_source.filter))

        if num_page > 1:
            for i in range(num_page-1):
                data_next = json.loads(json.dumps(zeep.helpers.serialize_object(data.next().data), default=json_serial))
                df = df.append(pd.DataFrame(use_jq_to_parse(data_next, data_source.filter)), ignore_index=True)

        return df

