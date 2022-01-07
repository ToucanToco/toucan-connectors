# Postgres connector

Import data from PostgreSQL.

## Data provider configuration

* `type`: `"Postgres"`
* `name`: str, required
* `user`: str, required
* `host` : str, required - either a host name (eg aeaze.toucan.com) or anIP address (e.g. 1.2.3.4)
* `charset`: str
* `password`: str
* `port`: int
* `connect_timeout`: int

```coffee
DATA_PROVIDERS: [
  type:    'Postgres'
  name:    '<name>'
  user:    '<user>'
  host:    '<host>'
  charset:    '<charset>'
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
* `database`: str, required
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
