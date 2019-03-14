# GoogleMyBusiness connector

## Data provider configuration

* `type`: `"GoogleMyBusiness"`
* `name`: str, required
* `credentials`: required
  * `token`: str
  * `refresh_token`: str
  * `token_uri`: str
  * `client_id`: str
  * `client_secret`: str
* `scopes`: list of str, default to ['https://www.googleapis.com/auth/business.manage']

```coffee
DATA_PROVIDERS: [
  type:    'GoogleMyBusiness'
  name:    '<name>'
  credentials:    '<credentials>'
  scopes:    ['<scope>']
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required. Should match the data provider name
* `metric_requests`: list of Metric (see below)
* `time_range`: required
  * `start_time`: str
  * `end_time`: str
* `location_ids`: list of str, optional. Defaults to all locations available.


**Metric**

* `metric`: str, required
* `options`, list of str, optional.


```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  metric_requests: [
    metric: 'QUERIES_DIRECT'
  ,
    metric: 'QUERIES_INDIRECT'
  ]
  time_range:
    start_time: '2019-01-27T00:00:00.045123456Z'
    end_time: '2019-02-27T23:59:59.045123456Z'
,
  ...
]
```
