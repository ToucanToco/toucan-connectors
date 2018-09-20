module 'toucan_connectors' has no attribute 'HttpApiConnector'
module 'toucan_connectors' has no attribute 'ToucanTocoConnector'
# HttpAPI connector

## Data provider configuration

* `type`: `"HttpAPI"`
* `name`: str, required
* `baseroute`: str, required
* `auth`: `{type: "basic|digest|oauth1", args: [...]}` 
    cf. [requests auth](http://docs.python-requests.org/en/master/) doc. 

```coffee
DATA_PROVIDERS= [
  type:    'HttpAPI'
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
* `url`: str, required
* `method`: Method, default to GET
* `headers`: dict
* `params`: dict
* `data`: str
* `filter`: str, [`jq` filter](https://stedolan.github.io/jq/manual/), default to `"."
* `auth`: Auth
* `parameters`: dict

```coffee
DATA_SOURCES= [
  domain:    '<domain>'
  name:    '<name>'
  url:    '<url>'
  method:    '<method>'
  headers:    '<headers>'
  params:    '<params>'
  data:    '<data>'
  filter:    '<filter>'
  auth:    '<auth>'
  parameters:    '<parameters>'
,
  ...
]
```
