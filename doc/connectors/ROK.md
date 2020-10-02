# ROK connector

Connector to [ROK](https://www.rok-solution.fr/).

## Data provider configuration

* `type`: `"ROK"`
* `name`: str, required
* `host`: str, required
* `username`: str, required
* `password`: str, required

```coffee
DATA_PROVIDERS: [
  type:    'ROK'
  name:    '<name>'
  host:    'https://rok.example.com'
  username: '<username>',
  password: '<password>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `database`: str, required
* `query`: GQL str, required
* `filter`: str, [`jq` filter](https://stedolan.github.io/jq/manual/), required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  database: '<database>'
  query: '<query>'
  filter:    '<filter>'
,
  ...
]
```
