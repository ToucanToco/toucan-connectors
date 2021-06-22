# One Drive connector

Import data from One Drive API (Microsoft Graph API).

## Data provider configuration

* `type`: `"OneDrive"`
* `name`: str, required
* `auth_flow_id`: str

The `auth_flow_id` will be used to identify tokens relative to this connector in the secrets database.

```javascript
DATA_PROVIDERS: [
  type:         'OneDrive'
  name:         '<name>'
  auth_flow_id:    '<auth_flow_id>'
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `file`: str
* `sheet`: str
* `range`: str, optional. Range represents a set of one or more contiguous cells such as a cell, a row, a column, block of cells, etc. Ex 'A2:B3'

```javascript
DATA_SOURCES: [
  domain: '<domain>',
  name: '<name>',
  file: '<file>',
  sheet: '<sheet>',
  range: '<range>',
]
```
