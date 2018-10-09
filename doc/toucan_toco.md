# ToucanToco connector

Get data from a Toucan Toco instance, usefull to build analytics applications.

## Data provider configuration

* `type`: `"ToucanToco"`
* `name`: str, required
* `host`: str, required
* `username`: str, required
* `password`: str, required

```coffee
DATA_PROVIDERS: [
  type:    'ToucanToco'
  name:    '<name>'
  host:    '<host>'
  username:    '<username>'
  password:    '<password>'
,
  ...
]
```


## Data source configuration

Use the `all_small_apps` parameter to get results from an endpoint on all small apps.

* `domain`: str, required
* `name`: str, required
* `endpoint`: Endpoints, required
* `all_small_apps`: bool, default to False

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  endpoint:    '<endpoint>'
  all_small_apps:    '<all_small_apps>'
,
  ...
]
```
