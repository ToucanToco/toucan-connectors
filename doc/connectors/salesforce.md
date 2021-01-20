# Salesforce connector

Import data from Salesforce API.

## Data provider configuration

* `type`: `"Salesforce"`
* `_auth_flow`: str
* `auth_flow_id`: str
* `instance_url`: str
  


The `_auth_flow` property marks this as being a connector that requires initiating the oauth dance and prevents it from being in the schema.
The `auth_flow_id` property is like an identifier that is used to identify the secrets associated with the connector.
The `instance_url` property is the url of your Salesforce instance


```coffee
DATA_PROVIDERS: [
  type:    'Salesforce'
  auth_flow_id: '<auth_flow_id>'
  instance_url: '<instance_url>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str (not empty), required

The `query` must be a valid SOQL query. See [documentation](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_sosl_intro.htm)

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'

,
  ...
]
```
