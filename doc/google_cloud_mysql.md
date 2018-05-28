# GoogleCloudMySQL connector

Import data from Google Cloud MySQL database.

## Data provider configuration

* `type`: `"GoogleCloudMySQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `db`: str, required
* `password`: str, required
* `port`: int, default to None
* `charset`: str, default to utf8mb4
* `connect_timeout`: int, default to None

```coffee
DATA_PROVIDERS= [
  type:    'GoogleCloudMySQL'
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

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `query`: ConstrainedStrValue, required

```coffee
DATA_SOURCES= [
  type:    'GoogleCloudMySQL'
  domain:    '<domain>'
  name:    '<name>'
  load:    '<load>'
  query:    '<query>'
,
  ...
]
```