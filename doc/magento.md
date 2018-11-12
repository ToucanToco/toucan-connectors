# Magento connector

Tested on Magento 1.x, list of endpoints [here](https://devdocs.magento.com/guides/m1x/api/soap/introduction.html).

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
