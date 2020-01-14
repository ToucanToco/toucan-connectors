# Lightspeed connector

Lightspeed Connector using Bearer to handle all the OAuth connection

## Data provider configuration

* `type`: `"Lightspeed"`
* `name`: str, required
* `bearer_auth_id`: str, required.

The `bearer_auth_id` is a token retrieved by the front-end
to let Bearer handle the OAuth dance for you.

```coffee
DATA_PROVIDERS: [
  type:    'Lightspeed'
  name:    '<name>'
  bearer_auth_id: '<bearer_auth_id>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `endpoint`: str, required
* `filter`: str, optional.

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  endpoint:    '<endpoint>'
,
  ...
]
```
