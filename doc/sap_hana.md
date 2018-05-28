# SapHana connector

Import data from Sap Hana.

## Data provider configuration

* `type`: `"SapHana"`
* `name`: str, required
* `host`: str, required
* `port`: str, required
* `user`: str, required
* `password`: str, required

```coffee
DATA_PROVIDERS= [
  type:    'SapHana'
  name:    '<name>'
  host:    '<host>'
  port:    '<port>'
  user:    '<user>'
  password:    '<password>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `query`: ConstrainedStrValue, required

```coffee
DATA_SOURCES= [
  type:    'SapHana'
  domain:    '<domain>'
  name:    '<name>'
  load:    '<load>'
  query:    '<query>'
,
  ...
]
```