# Dataiku connector

This is a basic connector for [Dataiku](https://www.dataiku.com/) using their 
[DSS API](https://doc.dataiku.com/dss/2.0/api/index.html).

## Connector configuration

* `type`: `"Dataiku"`
* `name`: str, required
* `host`: str, required
* `apiKey`: str, required
* `project`: str, required


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `dataset`: str, required
