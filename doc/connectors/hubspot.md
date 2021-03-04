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
  name:    'Hubspot Connector'
  auth_flow_id:    '<auth_flow_id>'
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, name given to the Hubspot provider as defined in `DATA_PROVIDERS`
* `dataset`: HubspotDataset
  * Possible values:
    * `contacts` (default value)
    * `content`
    * `products`
    * `web-analytics`
* `object_type`: HubspotObjectType, optional but needed for the `web-analytics` dataset
  * Possible values:
    * `contact`

```coffee
DATA_SOURCES: [
  domain:    'example_domain'
  name:    'Hubspot Connector'
  dataset:    'web-analytics'
  object_type: 'contact'
,
  ...
]
```
