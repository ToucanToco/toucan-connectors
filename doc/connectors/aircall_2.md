# Aircall2 connector

## Data provider configuration

* `type`: `"Aircall"`
* `name`: str, required
* `_auth_flow_id`: str
* `_auth_flow`: str, required

The `bearer_auth_id` is a token retrieved by the front-end
to let Bearer handle the OAuth dance for you.

```coffee
DATA_PROVIDERS: [
  type:    'Aircall'
  name:    '<name>'
  auth_flow_id: '<auth_flow_id>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `dataset`: str, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  dataset:    '<endpoint>'
,
  ...
]
```