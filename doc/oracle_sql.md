# OracleSQL connector

## Data provider configuration

* `type`: `"OracleSQL"`
* `name`: str, required
* `dsn`: DSN, required
* `user`: str, default to None
* `password`: str, default to None
* `encoding`: str, default to None

```coffee
DATA_PROVIDERS= [
  type:    'OracleSQL'
  name:    '<name>'
  dsn:    <dsn>
  user:    '<user>'
  password:    '<password>'
  encoding:    '<encoding>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `query`: str, required

```coffee
DATA_SOURCES= [
  type:    'OracleSQL'
  domain:    '<domain>'
  name:    '<name>'
  load:    '<load>'
  query:    '<query>'
,
  ...
]
```