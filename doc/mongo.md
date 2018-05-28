# MongoDB connector

Retreive data from a [MongoDB](https://www.mongodb.com/) database.

## Data provider configuration

* `type`: `"MongoDB"`
* `name`: str, required
* `host`: str, required
* `port`: int, required
* `database`: str, required
* `username`: str, default to None
* `password`: str, default to None

```coffee
DATA_PROVIDERS= [
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
* `query`: typing.Union[str, dict, list], required

```coffee
DATA_SOURCES= [
  type:    'MongoDB'
  domain:    '<domain>'
  name:    '<name>'
  collection:    '<collection>'
  query:    '<query>'
,
  ...
]
```