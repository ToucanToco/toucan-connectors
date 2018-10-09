# AdobeAnalytics connector

Adobe Analytics Connector using Adobe Analytics' REST API v1.4.
It provides a high-level interfaces for reporting queries (including Data Warehouse requests).

## Data provider configuration

* `type`: `"AdobeAnalytics"`
* `name`: str, required
* `username`: str, required
* `password`: str, required
* `endpoint`: str, default to https://api.omniture.com/admin/1.4/rest/

```coffee
DATA_PROVIDERS: [
  type:    'AdobeAnalytics'
  name:    '<name>'
  username:    '<username>'
  password:    '<password>'
  endpoint:    '<endpoint>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `suite_id`: str, required
* `dimensions`: str, list of str or list of dict
* `metrics`: str or list of str, required
* `date_from`: str, required
* `date_to`: str, required
* `segments`: str or list of str, default to None
* `last_days`: int
* `granularity`: `hour`, `day`, `week`, `month`, `quarter`, `year`, default to None
* `source`: str

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  suite_id:    '<suite_id>'
  dimensions:    '<dimensions>'
  metrics:    '<metrics>'
  date_from:    '<date_from>'
  date_to:    '<date_to>'
  segments:    '<segments>'
  last_days:    '<last_days>'
  granularity:    '<granularity>'
  source:    '<source>'
,
  ...
]
```
