# MSSQL connector

Import data from Microsoft SQL Server.

## Data provider configuration

* `type`: `"MSSQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `password`: str
* `port`: int
* `connect_timeout`: int

```coffee
DATA_PROVIDERS: [
  type:    'MSSQL'
  name:    '<name>'
  host:    '<host>'
  user:    '<user>'
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
* `database`: str
* `query`: str (not empty), required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  database:    '<database>'
  query:    '<query>'
,
  ...
]
```
