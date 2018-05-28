# MSSQL connector

Import data from Microsoft SQL Server.

## Data provider configuration

* `type`: `"MSSQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `db`: str, default to None
* `password`: str, default to None
* `port`: int, default to None
* `connect_timeout`: int, default to None

```coffee
DATA_PROVIDERS= [
  type:    'MSSQL'
  name:    '<name>'
  host:    '<host>'
  user:    '<user>'
  db:    '<db>'
  password:    '<password>'
  port:    <port>
  connect_timeout:    <connect_timeout>
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `query`: ConstrainedStrValue, required

```coffee
DATA_SOURCES= [
  type:    'MSSQL'
  domain:    '<domain>'
  name:    '<name>'
  load:    '<load>'
  query:    '<query>'
,
  ...
]
```