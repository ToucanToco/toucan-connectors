# GoogleSpreadsheet connector

## Data provider configuration

* `type`: `"GoogleSpreadsheet"`
* `name`: str, required
* `credentials`: GoogleCredentials, required
* `scope`: str, default to ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets', 'https://spreadsheets.google.com/feeds']

```coffee
DATA_PROVIDERS= [
  type:    'GoogleSpreadsheet'
  name:    '<name>'
  credentials:    <credentials>
  scope:    '<scope>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `spreadsheet_id`: str, required
* `sheetname`: str, default to None

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