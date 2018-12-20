# GoogleBigQuery connector

## Data provider configuration

* `type`: `"GoogleBigQuery"`
* `name`: str, required
* `credentials`: [GoogleCredentials](google_credentials.md), required
* `dialect`: Dialect, default to legacy
* `scopes`: list(str), default to ["https://www.googleapis.com/auth/bigquery"]

```coffee
DATA_PROVIDERS: [
  type:    'GoogleBigQuery'
  name:    '<name>'
  credentials:    '<credentials>'
  dialect:    '<dialect>'
  scopes:    '<scopes>'
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
