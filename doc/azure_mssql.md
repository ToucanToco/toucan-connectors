# AzureMSSQL connector

Import data from Microsoft Azure SQL Server.

## Data provider configuration

* `type`: `"AzureMSSQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `password`: str, required
* `db`: str, required
* `connect_timeout`: int

```coffee
DATA_PROVIDERS: [
  type:    'AzureMSSQL'
  name:    '<name>'
  host:    '<host>'
  user:    '<user>'
  password:    '<password>'
  db:    '<db>'
  connect_timeout:    <connect_timeout>
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