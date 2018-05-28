# Snowflake connector

## Data provider configuration

* `type`: `"Snowflake"`
* `name`: str, required
* `user`: str, required
* `password`: str, required
* `account`: str, required
* `ocsp_response_cache_filename`: Path, default to None

```coffee
DATA_PROVIDERS= [
  type:    'Snowflake'
  name:    '<name>'
  user:    '<user>'
  password:    '<password>'
  account:    '<account>'
  ocsp_response_cache_filename:    <ocsp_response_cache_filename>
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `query`: ConstrainedStrValue, required
* `database`: str, default to None
* `warehouse`: str, default to None

```coffee
DATA_SOURCES= [
  type:    'Snowflake'
  domain:    '<domain>'
  name:    '<name>'
  load:    '<load>'
  query:    '<query>'
  database:    '<database>'
  warehouse:    '<warehouse>'
,
  ...
]
```