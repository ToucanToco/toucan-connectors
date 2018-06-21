# ToucanToco connector

Get data from a Toucan Toco instance, usefull to build analytics applications.

## Connector configuration

* `type`: `"ToucanToco"`
* `name`: str, required
* `host`: str, required
* `username`: str, required
* `password`: str, required


## Data source configuration

Use the `all_small_apps` parameter to get results from an endpoint on all small apps.

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `endpoint`: Endpoints, required
* `all_small_apps`: bool, default to False