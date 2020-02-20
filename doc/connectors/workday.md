# Workday connector

This is a connector to get data from Workday, standards-based SOAP API. (cf. Workday API doc https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html)

This type of data source combines the features of Python’s [requests](http://docs.python-requests.org/) 
library to get data from any API with the filtering langage [jq](https://stedolan.github.io/jq/) for
flexbile transformations of the responses.

## Data provider configuration

* `type`: `"Workday"`
* `name`= str, required, name of the connector in ToucanToco (example: "Workday_Human_Resources")
* `tenant`= str, required, name of the Workday tenant
* `username`= str, required
* `password`= str, required


```coffee
DATA_PROVIDERS: [
  type: 'Workday'
  name: '<name>',
  tenant: '<tenant>',
  username: '<username>',
  password: '<password>'
,
  ...
]
```


## Data source configuration

* `name`: str, required, name of the datasource in ToucanToco (example: "Workday_Human_Resources_Get_Workers")
* `domain`: str, required, name of the domain that will be created on Toucan Toco
* `service`: str, required, name of the Workday API Service you want to use (please note that this is an arbitrary name, you can set whatever string you want. example: "Human_Resources")
* `service_WSDL_URL`: str, required, URL of the WSDL of the Workday API Service you want to use
* `operation`: str, required, name of the Workday API Operation you want to call within the selected Service
* `request_references_param`: dict, request references for the query
* `request_criteria_param`: dict, request criteria for the query
* `response_filter_param`: dict, response filter for the query
* `response_group_param`: dict, response group for the query
* `filter`: str, use the JQ language to slice the JSON response 

```coffee
DATA_SOURCES: [
  name: '<name>'
  domain: '<domain>'
  service: '<service>'
  service_WSDL_URL: '<service_WSDL_URL>'
  operation: '<operation>'
  request_references_param: '<request_references_param>'
  request_criteria_param: '<request_criteria_param>'
  response_filter_param: '<response_filter_param>'
  response_group_param: '<response_group_param>'
  filter: '<filter>'
,
  ...
]
```

## Example


```coffee
DATA_PROVIDERS: [
	name='myWorkdayConnector',
    type='Workday',
    tenant='umanis',
    username='username',
    password='********'
,
  ...
]
```


```coffee
DATA_SOURCES: [
    name='myWorkdayDataSource',
    domain='Workers',
    service='Human_Resources',
    service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2',
    operation='Get_Workers',
    request_references_param ={
       'Worker_Reference' : [
            {
                'ID' : {
                    '_value_1': '9836946ba6f401b18fe9c3a98d21ead6',
                    'type': 'WID'
                }
            },
            {
                'ID' : {
                    '_value_1': '9836946ba6f40107d53788a88d2174d4',
                    'type': 'WID'
                }
            }
        ]
    },
    request_criteria_param = {'Exclude_Inactive_Workers' : False},
    response_filter_param = None,
    response_group_param= {
        'Include_Reference' : True,
        'Include_Absence_Input_Data' : True
    },
    filter="[.Worker[].Worker_Data | {User_ID: .User_ID, Worker_ID: .Worker_ID}]"
,
  ...
]
```


