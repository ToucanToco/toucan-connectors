# AzureMSSQL connector

Import data from Microsoft Azure SQL Server.

## Connector configuration

* `type`: `"AzureMSSQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `password`: str, required
* `db`: str, required
* `connect_timeout`: int, default to None


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str (not empty), required
