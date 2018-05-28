# MySQL connector

Import data from MySQL database.

## Data provider configuration

* `type`: `"MySQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `db`: str, required
* `password`: str, default to None
* `port`: int, default to None
* `charset`: str, default to utf8mb4
* `connect_timeout`: int, default to None

```coffee
DATA_PROVIDERS= [
  type:    'MySQL'
  name:    '<name>'
  host:    '<host>'
  user:    '<user>'
  db:    '<db>'
  password:    '<password>'
  port:    <port>
  charset:    '<charset>'
  connect_timeout:    <connect_timeout>
,
  ...
]
```


## Data source configuration

Either `query` or `table` are required, both at the same time are not supported.

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `query`: ConstrainedStrValue, default to None
* `table`: ConstrainedStrValue, default to None

```coffee
DATA_SOURCES= [
  type:    'MySQL'
  domain:    '<domain>'
  name:    '<name>'
  load:    '<load>'
  query:    '<query>'
  table:    '<table>'
,
  ...
]
```