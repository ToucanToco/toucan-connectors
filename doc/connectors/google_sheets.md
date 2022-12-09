# GoogleSheets connector

## Data provider configuration

* `type`: `"GoogleSheets"`
* `name`: str, required
* `auth_id`: str, required

Note: this connector needs a `retrieve_token` function that will be given the `auth_id`. It may either return an access
token as a string, or a dict containing an access token and a refresh token:


```python
GoogleSheetsConnector(
    name='<name>',
    auth_id='<auth_id>',
    retrieve_token=lambda auth_id: '<valid_access_token>',
    # OR
    retrieve_token=lambda auth_id: {
        'token': '<valid_access_token>',
        'refresh_token': '<valid_refresh_token',
    },
)
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
  :    '<spreadsheet_id>'
  sheetname:    '<sheetname>'
  skip_rows:    <skip_rows>
,
  ...
]
```

```python
GoogleSheetsDataSource(
    name='<name>',
    domain='<domain>',
    spreadsheet_id='<spreadsheet_id>',
    sheet='<sheet>',
    header_row='<header_row>',
)
```

## Additional methods

### `list_sheets`

Arguments:
- `spreadsheet_id`: `str`

List available sheets of a particular spreadsheet
