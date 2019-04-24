# MicroStrategy connector

Import data from MicroStrategy using the [JSON Data API](http://bit.ly/2HCzf04) for cubes and
    reports.

## Data provider configuration

* `type`: `"MicroStrategy"`
* `name`: str, required
* `base_url`: str, required
* `username`: str, required
* `password`: str, required
* `project_id`: str, required

```coffee
DATA_PROVIDERS: [
  type:    'MicroStrategy'
  name:    '<name>'
  base_url:    '<base_url>'
  username:    '<username>'
  password:    '<password>'
  project_id:    '<project_id>'
,
  ...
]
```


## Data source configuration

Specify whether you want to use the `cube` or `reports` endpoints and a microstrategy doc id.

* `domain`: str, required
* `name`: str, required
* `id`: str, required
* `dataset`: str, `cube` or `report, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  id:    '<id>'
  dataset:    '<dataset>'
,
  ...
]
```