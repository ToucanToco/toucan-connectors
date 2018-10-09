# Dataiku connector

This is a basic connector for [Dataiku](https://www.dataiku.com/) using their
    [DSS API](https://doc.dataiku.com/dss/2.0/api/index.html).

## Data provider configuration

* `type`: `"Dataiku"`
* `name`: str, required
* `host`: str, required
* `apiKey`: str, required
* `project`: str, required

```coffee
DATA_PROVIDERS: [
  type:    'Dataiku'
  name:    '<name>'
  host:    '<host>'
  apiKey:    '<apiKey>'
  project:    '<project>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `dataset`: str, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  dataset:    '<dataset>'
,
  ...
]
```