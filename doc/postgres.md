# Postgres connector

Import data from PostgreSQL.

## Data provider configuration

* `type`: `"Postgres"`
* `name`: str, required
* `user`: str, required
* `host`: str
* `hostname`: str
* `charset`: str
* `db`: str
* `password`: str
* `port`: int
* `connect_timeout`: int

```coffee
DATA_PROVIDERS= [
  type:    'Postgres'
  name:    '<name>'
  user:    '<user>'
  host:    '<host>'
  hostname:    '<hostname>'
  charset:    '<charset>'
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
* `query`: str (not empty), required

```coffee
DATA_SOURCES= [
  type:    'Postgres'
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
,
  ...
]
```