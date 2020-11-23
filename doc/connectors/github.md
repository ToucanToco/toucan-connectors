# Github connector

This is a connector for [Github](https://docs.github.com/en/free-pro-team@latest/graphql)
using oAuth for authentication. It retrieves Pull Requests and Teams dataset

A quickstart script for oAuth authentication is available in toucan-connectors/quickstart/github/quickstart.py

## Data provider configuration

* `name`: str, required
* `auth_flow_id`: str

The `auth_flow_id` will be used to identify tokens relative to this connector in the secrets database.

```coffee
DATA_PROVIDERS: [
  name:    '<name>'
  auth_flow_id:    '<auth_flow_id>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `dataset`: GithubDataset, default to teams
* `organization`: str, The organization which datasets will be extracted

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  dataset:    '<dataset>'
  organization: '<organization>'
,
  ...
]
```
