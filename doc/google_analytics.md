# GoogleAnalytics connector

## Data provider configuration

* `type`: `"GoogleAnalytics"`
* `name`: str, required
* `credentials`: [GoogleCredentials](google_credentials.md), required
* `scope`: list of str, default to ['https://www.googleapis.com/auth/analytics.readonly']

```coffee
DATA_PROVIDERS: [
  type:    'GoogleAnalytics'
  name:    '<name>'
  credentials:  {
    type: '<type>'
    project_id: '<project_id>'
    ...  # see documentation below
  }
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
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  report_request:    '<report_request>'
,
  ...
]
```
