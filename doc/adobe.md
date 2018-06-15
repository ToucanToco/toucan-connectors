# AdobeAnalytics connector

Adobe Analytics Connector using Adobe Analytics' REST API v1.4.
It provides a high-level interfaces for reporting queries (including Data Warehouse requests).

## Connector configuration

* `type`: `"AdobeAnalytics"`
* `name`: str, required
* `username`: str, required
* `password`: str, required
* `endpoint`: str, default to https://api.omniture.com/admin/1.4/rest/


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `suite_id`: str, required
* `dimensions`: str, list of str or list of dict
* `metrics`: str or list of str, required
* `date_from`: str, required
* `date_to`: str, required
* `segments`: str or list of str, default to None
* `last_days`: int, default to None
* `granularity`: `hour`, `day`, `week`, `month`, `quarter`, `year`, default to None
* `source`: str, default to None
