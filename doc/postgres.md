# Postgres connector

Import data from PostgreSQL.

## Data provider configuration

* `type`: `"Postgres"`
* `name`: str, required
* `user`: str, required
* `host`: str, default to None
* `hostname`: str, default to None
* `charset`: str, default to None
* `db`: str, default to None
* `password`: str, default to None
* `port`: int, default to None
* `connect_timeout`: int, default to None

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
* `load`: bool, default to None
* `query`: ConstrainedStrValue, required

```coffee
DATA_SOURCES= [
  type:    'Postgres'
  domain:    '<domain>'
  name:    '<name>'
  load:    '<load>'
  query:    '<query>'
,
  ...
]
```