# Aircall connector

This is a connector for [Aircall](https://developer.aircall.io/api-references/#endpoints)
    using oAuth for authentication

A quickstart script is available in toucan-connectors/doc/connectors/quickstart_aircall.py

## Data provider configuration

* `type`: `"Aircall"`
* `name`: str, required
* `type`: str
* `auth_flow_id`: str

The `auth_flow_id` will be used to identify tokens relative to this connector in the secrets database.

```coffee
DATA_PROVIDERS: [
  type:    '<type>'
  name:    '<name>'
  auth_flow_id:    '<auth_flow_id>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `limit`: int (not empty), default to 1
* `dataset`: AircallDataset, default to calls

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
