# Odbc connector

Import data from ODBC apis

## Data provider configuration

* `type`: `"Odbc"`
* `name`: str, required
* `type`: str
* `connection_string`: str, required
* `autocommit`: bool, default to False
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
