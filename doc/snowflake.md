# Snowflake connector

Import data from Snowflake data warehouse.

## Data provider configuration

* `type`: `"Snowflake"`
* `name`: str, required
* `user`: str, required
* `password`: str, required
* `account`: str, required
* `ocsp_response_cache_filename`: Path

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
* `query`: str (not empty), required
* `database`: str
* `warehouse`: str

```coffee
DATA_SOURCES= [
  type:    'Snowflake'
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
  database:    '<database>'
  warehouse:    '<warehouse>'
,
  ...
]
```