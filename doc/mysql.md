# MySQL connector

Import data from MySQL database.

## Data provider configuration

* `type`: `"MySQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `db`: str, required
* `password`: str
* `port`: int
* `charset`: str, default to utf8mb4
* `connect_timeout`: int

```coffee
DATA_PROVIDERS: [
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
* `query`: str (not empty), required if `table` is not provided.
* `table`: str (not empty), required if `query` is not provided, will read the whole table.
* `follow_relations`: bool, default to false. Merges data from foreign key relations.
* `parameters` dict, optional. Allow to parameterize the query.

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    'SELECT * FROM city WHERE country = %(country)s'
  parameters:
    country: '<value>'
,
  ...
]
```
