# Dataiku connector

## Data provider configuration

* `type`: `"Dataiku"`
* `name`: str, required
* `host`: str, required
* `apiKey`: str, required
* `project`: str, required

```coffee
DATA_PROVIDERS= [
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
DATA_SOURCES= [
  type:    'Dataiku'
  domain:    '<domain>'
  name:    '<name>'
  dataset:    '<dataset>'
,
  ...
]
```