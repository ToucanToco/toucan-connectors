# OData connector

## Data provider configuration

* `type`: `"OData"`
* `name`: str, required
* `baseroute`: str, required
* `auth`: `{type: "basic|digest|oauth1|oauth2_backend|custom_token_server", args: [...], kwargs: {...}}` 
    cf. [requests auth](http://docs.python-requests.org/en/master/) and 
    [requests oauthlib](https://requests-oauthlib.readthedocs.io/en/latest/oauth2_workflow) doc. 

```coffee
DATA_PROVIDERS: [
  type:    'OData'
  name:    '<name>'
  baseroute:    '<baseroute>'
  auth:    '<auth>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `entity`: str, required
* `query`: dict, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  entity:    '<entity>'
  query:    '<query>'
,
  ...
]
```
