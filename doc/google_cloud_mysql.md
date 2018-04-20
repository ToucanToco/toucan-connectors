# GoogleCloudMySQL connector

Import data from Google Cloud MySQL database.

## Connector configuration

* `type`: `"GoogleCloudMySQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `db`: str, required
* `password`: str, required
* `port`: int, default to None
* `charset`: str, default to utf8mb4
* `connect_timeout`: int, default to None


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str (not empty), required
