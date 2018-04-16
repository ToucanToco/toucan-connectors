# MongoDB connector

Retrieve data from a [MongoDB](https://www.mongodb.com/) database.

## Connector configuration

* `type`: `"MongoDB"`
* `name`: str, required
* `host`: str, required
* `port`: int, required
* `database`: str, required
* `username`: str, default to None
* `password`: str, default to None


## Data source configuration

Supports simple, multiples and aggregation queries as desribed in
[our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)

* `domain`: str, required
* `name`: str, required
* `collection`: str, required
* `query`: `str` (translated to a query `{domain: <value>}`), dict or list, required
