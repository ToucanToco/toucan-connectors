# Magento connector

Tested on Magento 1.x, list of available `resource_path` is [here](https://devdocs.magento.com/guides/m1x/api/soap/introduction.html).
Often the relevant `arguments` is going to be one list of filters 
used in a search : `[{"status": "pending"}]` for a list or orders for example. The underlying search 
engine is [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html) 
if you are looking for the operators available.

## Data provider configuration

* `type`: `"Magento"`
* `name`: str, required
* `url`: str, required
* `username`: str, required
* `password`: str, required

```coffee
DATA_PROVIDERS: [
  type:    'Magento'
  name:    '<name>'
  url:    '<url>'
  username:    '<username>'
  password:    '<password>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `resource_path`: str, required, e.g. 'sales_order.list'
* `arguments`: list, default to []

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  resource_path:    '<resource_path>'
  arguments:    '<arguments>'
,
  ...
]
```
