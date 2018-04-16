# Snowflake connector

Import data from Snowflake data warehouse.

## Connector configuration

* `type`: `"Snowflake"`
* `name`: str, required
* `user`: str, required
* `password`: str, required
* `account`: str, required
* `ocsp_response_cache_filename`: str, path to the location used to store [ocsp cache] (https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#caching-ocsp-responses)


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str, required
* `database`: str
* `warehouse`: str
