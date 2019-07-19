# Postgres connector

Import data from PostgreSQL.

## Data provider configuration

* `type`: `"Postgres"`
* `name`: str, required
* `user`: str, required
* `host` or `hostname` : str, required
  - If you have a host name (eg aeaze.toucan.com), use the `hostname` parameter
  - If you have an IP address (e.g. 1.2.3.4), use the `host` parameter
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
  hostname:    '<hostname>'
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
