# MicroStrategy connector

Import data from MicroStrategy using the [JSON Data API](http://bit.ly/2HCzf04) for cubes and
reports.

## Connector configuration

* `type`: `"MicroStrategy"`
* `name`: str, required
* `base_url`: str, required
* `username`: str, required
* `password`: str, required
* `project_id`: str, required


## Data source configuration

Specify whether you want to use the `cube` or `reports` endpoints and a microstrategy doc id.

* `domain`: str, required
* `name`: str, required
* `id`: str, required
* `dataset`: str, `cube` or `report, required
