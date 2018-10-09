# MongoDB connector

Retrieve data from a [MongoDB](https://www.mongodb.com/) database.
```eval_rst
  This is the connector used to retrieve loaded data in Toucan Toco interface.
```

## Data provider configuration

* `type`: `"MongoDB"`
* `name`: str, required
* `host`: str, required
* `port`: int, required
* `database`: str, required
* `username`: str
* `password`: str

```coffee
DATA_PROVIDERS: [
  type:    'MongoDB'
  name:    '<name>'
  host:    '<host>'
  port:    <port>
  database:    '<database>'
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
* `collection`: str, required
* `query`: `str` (translated to a query `{domain: <value>}`), dict or list, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  collection:    '<collection>'
  query:    '<query>'
,
  ...
]
```
