# Snowflake connector

Import data from Snowflake data warehouse.

## Connector configuration

* `type`: `"Snowflake"`
* `name`: str, required
* `user`: str, required
* `password`: str, required
* `account`: str, required
* `ocsp_response_cache_filename`: Path, default to None


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str, required
* `database`: str, default to None
* `warehouse`: str, default to None
