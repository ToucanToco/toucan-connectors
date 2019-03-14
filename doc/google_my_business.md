# GoogleMyBusiness connector

## Data provider configuration

* `type`: `"GoogleMyBusiness"`
* `name`: str, required
* `credentials`: required (see "get credentials" section below)
  * `token`: str
  * `refresh_token`: str
  * `token_uri`: str
  * `client_id`: str
  * `client_secret`: str
* `scopes`: list of str, default to ['https://www.googleapis.com/auth/business.manage']

```coffee
DATA_PROVIDERS: [
  type:    'GoogleMyBusiness'
  name:    '<name>'
  credentials:    '<credentials>'
  scopes:    ['<scope>']
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required. Should match the data provider name
* `metric_requests`: list of Metric (see below)
* `time_range`: required
  * `start_time`: str
  * `end_time`: str
* `location_ids`: list of str, optional. Defaults to all locations available.


**Metric**

* `metric`: str, required
* `options`, list of str, optional.


```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  metric_requests: [
    metric: 'QUERIES_DIRECT'
  ,
    metric: 'QUERIES_INDIRECT'
  ]
  time_range:
    start_time: '2019-01-27T00:00:00.045123456Z'
    end_time: '2019-02-27T23:59:59.045123456Z'
,
  ...
]
```


## Get credentials

First, you will need a valid `client_secret.json` file (you can download it from <INSERT EXPLANATION HERE>).

Then, in a virtualenv with `google_auth_oauthlib` and `google-api-python-client` package, you can use this python code to get your credentials:

```python
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

CLIENT_SECRETS_FILE = "client_secret.json"
SCOPES = ["https://www.googleapis.com/auth/business.manage"]

flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
credentials = flow.run_console()
json_credentials = json.dumps({
    "token": credentials.token,
    "refresh_token": credentials.refresh_token,
    "token_uri": credentials.token_uri,
    "client_id": credentials.client_id,
    "client_secret": credentials.client_secret,
}, indent=2)
print(json_credentials)
```
