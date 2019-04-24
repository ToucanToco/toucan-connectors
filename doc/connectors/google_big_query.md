# GoogleBigQuery connector

## Data provider configuration

* `type`: `"GoogleBigQuery"`
* `name`: str, required
* `credentials`: GoogleCredentials, required
* `dialect`: Dialect, default to legacy
* `scopes`: list(str), default to ["https://www.googleapis.com/auth/bigquery"]

### GoogleCredentials

For authentication, download an authentication file from console.developper.com
and use the values here. This is an oauth2 credential file. For more information
see this: http://gspread.readthedocs.io/en/latest/oauth2.html

* `type`: str
* `project_id`: str
* `private_key_id`: str
* `private_key`: str
* `client_email`: str
* `client_id`: str
* `auth_uri`: str
* `token_uri`: str
* `auth_provider_x509_cert_url`: str
* `client_x509_cert_url`: str


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
