import pandas as pd
import workday
from pydantic import Field

from toucan_connectors.common import FilterSchema, transform_with_jq
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

import decimal
import json
import types
from datetime import date, datetime

import zeep
from workday.auth import AnonymousAuthentication, WsSecurityCredentialAuthentication


def convert_zeep_object(obj):
    if isinstance(obj, zeep.xsd.valueobjects.CompoundValue):
        obj = zeep.helpers.serialize_object(obj)

    if isinstance(obj, list):
        return [convert_zeep_object(sub) for sub in obj]

    if isinstance(obj, dict):
        return {k: convert_zeep_object(v) for k, v in obj.items()}

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, decimal.Decimal):
        return str(obj)

    return obj

class WorkdayDataSource(ToucanDataSource):
    service: str = Field(
        None,
        title='Name of the Workday API Service you want to use.',
        description='Check "Public Web Services" page in Workday',
        example='Human_Resources',
    )
    service_WSDL_URL: str = Field(
        None,
        title='URL of the WSDL of the Workday API Service you want to use.',
        description='https://{Workday domain}/ccx/service/{tenant}/{service}/{version}?wsdl',
        example='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2?wsdl',
    )
    operation: str = Field(
        None,
        title='Name of the Workday API Operation you want to call within the selected Service.',
        description='Check https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html',
        example='Get_Workers',
    )
    request_parameters: dict = Field(
        None,
        title='Request References Parameter',
        description='Check https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html',
        example='{Request_References : None, Request_Criteria : None, Response_Filter : {}, Response_Group : None}',
    )
    filter: str = FilterSchema


class WorkdayConnector(ToucanConnector):
    data_source_model: WorkdayDataSource

    tenant: str = Field(
        None,
        title='Name of the Workday tenant',
        description='Name of the Workday tenant',
        example='my_tenant',
    )
    username: str = Field(
        None,
        title='Username for authentification',
        description='Username for Credential authentification. Leave empty for Anonymous authentification.',
        example='my_username',
    )
    password: str = Field(
        None,
        title='Password for authentification',
        description='Password for Credential authentification',
        example='$ecreTP4s$w0rd!',
    )

    def _retrieve_data(self, data_source: WorkdayDataSource) -> pd.DataFrame:
        """ Only supports Anonymous or Credentials authentification """

        if self.username:
            auth = WsSecurityCredentialAuthentication(
                '@'.join((self.username, self.tenant)), self.password
            )
        else:
            auth = AnonymousAuthentication()

        apis = {data_source.service: data_source.service_WSDL_URL}

        client = workday.WorkdayClient(wsdls=apis, authentication=auth)

        data = getattr(getattr(client, data_source.service), data_source.operation)(
            **data_source.request_parameters
        )

        num_page = data.total_pages

        data_json = convert_zeep_object(data.data)

        df = pd.DataFrame(transform_with_jq(data_json, data_source.filter))

        if num_page > 1:
            for i in range(num_page - 1):
                data_next = convert_zeep_object(data.next().data)
                df = df.append(
                    pd.DataFrame(transform_with_jq(data_next, data_source.filter)),
                    ignore_index=True,
                )

        return df
