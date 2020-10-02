# Wootric connector

Import data from [Wootric](https://www.wootric.com) API.

## Data provider configuration

* `type`: `"Wootric"`
* `name`: str, required
* `client_id`: str, required, represents the OAUTH client_id
* `client_secret`: str, required, represents the OAUTH client_secret
* `api_version`: str, optional, defaults to `"v1"`

```coffee
DATA_PROVIDERS: [
  type:    'Wootric'
  name:    '<name>'
  client_id:    'cc84asl8p980a9sjdlkxcz'
  client_secret: '5477aslk8clkaxcjlakscaas'
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str (not empty), required, the API endpoint (e.g. `responses`)
* `batch_size`: int, default to 5, how many HTTP queries should be batched together
  for a better optimization of network latency,
* `max_pages`: int, default to 30, maximum number of pages to fetch on the wootric API
  endpoint. For instance, wootric limit to `30` the number of pages for the `responses`
  endpoint so there would no point in putting a higher limit.
* `properties`: list of str, the response properties to filter out. Default is to
  fetch everything

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    'responses'
  batch_size: 10
,
  ...
]
```

For more information, check [the api documentation](https://docs.wootric.com/api)
