# MSSQL connector

Import data from Microsoft SQL Server.

## Connector configuration

* `type`: `"MSSQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `db`: str, default to None
* `password`: str, default to None
* `port`: int, default to None
* `connect_timeout`: int, default to None


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str (not empty), required
