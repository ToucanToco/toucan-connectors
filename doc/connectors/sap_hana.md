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
DATA_PROVIDERS: [
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
* `query`: str (not empty), required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
,
  ...
]
```