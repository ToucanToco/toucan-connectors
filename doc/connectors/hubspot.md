# HubSpot connector

This is a connector for [HubSpot](https://developers.hubspot.com/docs/api/developer-guides-resources) using OAuth for authentication.

## Data provider configuration

* `type`: `"Hubspot"`
* `name`: str, required
* `auth_flow_id`: str

The `auth_flow_id` will be used to identify tokens relative to this connector in the secrets database.


```coffee
DATA_PROVIDERS: [
  type:    'Hubspot'
  name:    '<name>'
  auth_flow_id:    '<auth_flow_id>'
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `dataset`: HubspotDataset, default to contacts
* `object_type`: HubspotObjectType, optional but needed for the `web-analytics` dataset

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  limit:    '<limit>'
  dataset:    '<dataset>'
,
  ...
]
```
