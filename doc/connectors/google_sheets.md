# GoogleSheets connector

## Data provider configuration

* `type`: `"GoogleSheets"`
* `name`: str, required
* `bearer_auth_id`: str, required

The `bearer_auth_id` is a token retrieved by the front-end
to let Bearer handle the OAuth dance for you.


```coffee
DATA_PROVIDERS: [
  type:    'GoogleSheets'
  name:    '<name>'
  bearer_auth_id: '<bearer_auth_id>'
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
  sheetname:    '<sheetname>'
  skip_rows:    <skip_rows>
,
  ...
]
```
