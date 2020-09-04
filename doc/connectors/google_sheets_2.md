# This is the doc for the Google Sheets 2 connector

## Data provider configuration

* `type`: `"GoogleSheets2"`
* `name`: str, required
* `auth_flow`: str
* `baseroute`: str
* `secrets`: dict

The `auth_flow` property marks this as being a connector that uses the connector_oauth_manager for the oauth dance.

The `baseroute` is fixed and is 'https://sheets.googleapis.com/v4/spreadsheets/'.

The `secrets` dictionary contains the `access_token` and a `refresh_token` (if there is one). Though `secrets` is optional during the initial creation of the connector, it is necessary for when the user wants to make requests to the connector. If there is no `access_token`, an Exception is thrown.


```coffee
DATA_PROVIDERS: [
  type:    'GoogleSheets'
  name:    '<name>'
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required. Should match the data provider name
* `spreadsheet_id`: str, required. Id of the spreadsheet which can be found inside
the url: https://docs.google.com/spreadsheets/d/<spreadsheet_id_is_here>/edit?pref=2&pli=1#gid=0,
* `sheet`: str. By default, the extractor returns the first sheet.
* `header_row`: int, default to 0. Row of the header of the spreadsheet


```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  spreadsheet_id:    '<spreadsheet_id>'
  sheet:    '<sheet name>'
  skip_rows:    <skip_rows>
,
  ...
]
```