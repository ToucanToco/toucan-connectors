# Postgres connector

Import data from PostgreSQL.

## Connector configuration

* `type`: `"Postgres"`
* `name`: str, required
* `user`: str, required
* `host`: str
* `hostname`: str
* `charset`: str
* `db`: str
* `password`: str
* `port`: int
* `connect_timeout`: int


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: non empty str, required
