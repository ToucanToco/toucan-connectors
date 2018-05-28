# OracleSQL connector

## Data provider configuration

* `type`: `"OracleSQL"`
* `name`: str, required
* `dsn`: DSN, required
* `user`: str
* `password`: str
* `encoding`: str

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
* `query`: str, required

```coffee
DATA_SOURCES= [
  type:    'OracleSQL'
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
,
  ...
]
```