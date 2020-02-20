# Workday connector

This is a connector to get data from Workday, standards-based SOAP API. (cf. Workday API doc https://community.workday.com/sites/default/files/file-hosting/productionapi/index.html)

This type of data source combines the features of Python’s [requests](http://docs.python-requests.org/) 
library to get data from any API with the filtering langage [jq](https://stedolan.github.io/jq/) for
flexbile transformations of the responses.

## Data provider configuration

* `type`: `"Workday"`
* `name`= str, required
* `tenant`= str, required
* `username`= str, required
* `password`= str, required


```coffee
DATA_PROVIDERS: [
  type: 'Workday'
  name: '<name>',
  tenant: '<tenant>',
  username: '<username>',
  password: '<password>!'
,
  ...
]
```


## Data source configuration

* `name`: str, required
* `domain`: str, required
* `service`: str, required
* `service_WSDL_URL`: str, required
* `operation`: str, required
* `request_references_param`: dict 
* `request_criteria_param`: dict 
* `response_filter_param`: dict 
* `response_group_param`: dict 
* `filter`: str 

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


