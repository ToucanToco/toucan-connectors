# GoogleAnalytics connector

## Data provider configuration

* `type`: `"GoogleAnalytics"`
* `name`: str, required
* `credentials`: GoogleCredentials, required
* `scope`: list of str, default to ['https://www.googleapis.com/auth/analytics.readonly']

```coffee
DATA_PROVIDERS= [
  type:    'GoogleAnalytics'
  name:    '<name>'
  credentials:    '<credentials>'
  scope:    '<scope>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `report_request`: dict, required (cf. https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet)

```coffee
DATA_SOURCES= [
  type:    'GoogleAnalytics'
  domain:    '<domain>'
  name:    '<name>'
  report_request:    '<report_request>'
,
  ...
]
```
