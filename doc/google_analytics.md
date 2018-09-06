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

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `report_request`: dict, required (cf. https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet)

```coffee
DATA_SOURCES= [
  domain:    '<domain>'
  name:    '<name>'
  report_request:    '<report_request>'
,
  ...
]
```
