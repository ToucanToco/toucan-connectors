# MySQL connector

Import data from MySQL database.

## Connector configuration

* `type`: `"MySQL"`
* `name`: str, required
* `host`: str, required
* `user`: str, required
* `db`: str, required
* `password`: str 
* `port`: int 
* `charset`: str, default to utf8mb4
* `connect_timeout`: int


## Data source configuration

Either `query` or `table` are required, both at the same time are not supported.

* `domain`: str, required
* `name`: str, required
* `query`: non empty str, required if `table` is not provided. 
* `table`: non empty str, required if `query` is not provided, will read the whole table. 
