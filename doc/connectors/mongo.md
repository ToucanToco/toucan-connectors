# MongoDB connector

Retrieve data from a [MongoDB](https://www.mongodb.com/) database.
This is the connector used to retrieve loaded data in Toucan Toco interface.

## Data provider configuration

* `type`: `"MongoDB"`
* `name`: str, required
* `host`: str, required
* `port`: int, required
* `username`: str
* `password`: str

```coffee
DATA_PROVIDERS: [
  type:    'MongoDB'
  name:    '<name>'
  host:    '<host>'
  port:    <port>
  username:    '<username>'
  password:    '<password>'
,
  ...
]
```


## Data source configuration

Supports simple, multiples and aggregation queries as desribed in
     [our documentation](https://docs.toucantoco.com/concepteur/data-sources/02-data-query.html)

* `domain`: str, required
* `name`: str, required
* `database`: str, required
* `collection`: str, required
* `query`: `str` (translated to a query `{domain: <value>}`), dict or list, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  database:    '<database>'
  collection:    '<collection>'
  query:    '<query>'
,
  ...
]
```

## Notes

### Context manager usage

The Mongo connector can be used as a context manager to avoid opening
and closing a connection to a same database.
For example:

```python
from toucan_connectors.mongo.mongo_connector import MongoConnector, MongoDataSource

queries = [
    {'domain': 'domain1', 'country': 'France'},
    {'domain': 'domain1', 'country': 'England'},
]

with MongoConnector(name='mycon', host='myhost', port=27017) as con:
    for query in queries:
        datasource = MongoDataSource(collection='test_col', query=query)
        con.get_df(datasource)
```

### Document count limit

The Mongo connectors limits the number of counted documents to one million, to
avoid scanning all results of a very large query at each `get_slice` call.
A count of 1M and 1 means that there is more than one million results.
