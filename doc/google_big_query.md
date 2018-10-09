# GoogleBigQuery connector

## Data provider configuration

* `type`: `"GoogleBigQuery"`
* `name`: str, required
* `credentials`: [GoogleCredentials](google_credentials.md), required
* `dialect`: Dialect, default to legacy

```coffee
DATA_PROVIDERS: [
  type:    'GoogleBigQuery'
  name:    '<name>'
  credentials:    '<credentials>'
  dialect:    '<dialect>'
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
,
  ...
]
```
