# This is the doc for the Google Sheets 2 connector

## Data provider configuration

* `type`: `"GoogleSheets2"`
* `name`: str, required
* `_auth_flow`: str
* `auth_flow_id`: str
* `_baseroute`: str

The `_auth_flow` property marks this as being a connector that requires initiating the oauth dance and prevents it from being in the schema.

The `_baseroute` is fixed and is 'https://sheets.googleapis.com/v4/spreadsheets/'. This is also hidden from rendering.

The `auth_flow_id` property is like an identifier that is used to identify the secrets associated with the connector.


```coffee
DATA_PROVIDERS: [
  type:    'GoogleSheets'
  name:    '<name>'
  auth_flow_id: '<auth_flow_id>'
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

Values are retrieved with the parameter [valueRenderOption](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get#body.QUERY_PARAMETERS.value_render_option) set to `UNFORMATTED_VALUE` to escape the rendering based on the locale defined in google sheets.

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
