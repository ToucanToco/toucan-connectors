# GoogleSpreadsheet connector

For authentication, download an authentication file from console.developper.com
    and use the values here. This is an oauth2 credential file. For more information
    see this: http://gspread.readthedocs.io/en/latest/oauth2.html

## Data provider configuration

* `type`: `"GoogleSpreadsheet"`
* `name`: str, required
* `credentials`: GoogleCredentials (see below), required
* `scope`: str, default to ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets', 'https://spreadsheets.google.com/feeds']

```coffee
DATA_PROVIDERS= [
  type:    'GoogleSpreadsheet'
  name:    '<name>'
  credentials:    '<credentials>'
  scope:    '<scope>'
,
  ...
]
```

### GoogleCredentials

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
* `spreadsheet_id`: str, required
* `sheetname`: str

```coffee
DATA_SOURCES= [
  type:    'GoogleSpreadsheet'
  domain:    '<domain>'
  name:    '<name>'
  spreadsheet_id:    '<spreadsheet_id>'
  sheetname:    '<sheetname>'
,
  ...
]
```
