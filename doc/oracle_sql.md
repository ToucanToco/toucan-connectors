# Oracle SQL connector

Import data from Oracle SQL database.

## Connector configuration

* `type`: `"OracleSQL"`
* `name`: str, required
* `dsn`: str following the DSN pattern, cf. https://en.wikipedia.org/wiki/Data_source_name, required
* `user`: str, default to None
* `password`: str, default to None
* `encoding`: str, default to None


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str, required
