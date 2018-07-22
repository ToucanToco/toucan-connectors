# GoogleAnalytics connector

## Connector configuration

* `type`: `"GoogleAnalytics"`
* `name`: str, required
* `credentials`: GoogleCredentials, required
* `scope`: str, default to ['https://www.googleapis.com/auth/analytics.readonly']


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `load`: bool, default to None
* `report_request`: ReportRequest, required
