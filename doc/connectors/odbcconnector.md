# Odbc connector

Import data through ODBC apis

Be aware that this connector needs a specific driver to be installed on the server for the data provider called.

## Data provider configuration

* `type`: `"Odbc"`
* `name`: str, required
* `connection_string`: str, required
* `ansi`: bool, default to False
* `connect_timeout`: int

```coffee
DATA_PROVIDERS: [
  type:    '<type>'
  name:    '<name>'
  connection_string:    '<connection_string>'
  autocommit:    '<autocommit>'
  ansi:    '<ansi>'
  connect_timeout:    '<connect_timeout>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str (not empty), required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
,
  ...
]
```
