# ROK connector

Connector to [ROK](https://www.rok-solution.fr/).

Two authentication methods are available:
- username with password
- username and a specific JWT, signed by the secret shared with the ROK instance

The later mode is useful to be able to do requests from multiple users with the same connector instance. However, be aware that the server using this must be trusted by ROK, as it can impersonate any user.
This trust is materialized by a shared secrets, provided by the ROK team.
A common practice would be to parametrize the username, and let only the trusted server to fill it.
An example of such configuration would be:
    {
      "type": "ROK",
      "name": "user_specific_ROK_data",
      "username": "{{ user.username }}",
      "authentified_with_rok_token": true,
      "secret": "xxxxxx",
      ...
    }

start_date, end_date and viewId can be specified in the data source's definition.
If set, the data source's query must have placeholders like %(start_date)s for this parameters to be used.

## Data provider configuration

* `type`: `"ROK"`
* `name`: str, required
* `host`: str, required
* `username`: str, default to None
* `password`: str, default to None
* `secret`: str, default to None
* `authenticated_with_token`: bool, default to False

```coffee
DATA_PROVIDERS: [
  type:    'ROK'
  name:    '<name>'
  host:    'https://rok.example.com'
  username: '<username>',
  password: '<password>',
  secret: '<secret>',
  authenticated_with_token: '<authenticated_with_token>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `database`: str, required
* `query`: GQL str, required
* `filter`: str, [`jq` filter](https://stedolan.github.io/jq/manual/), required
* `start_date`: str
* `end_date`: str
* `viewId`: str

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  database: '<database>'
  query: '<query>'
  filter:    '<filter>'
  start_date: '<start_date>'
  end_date: '<end_date>'
  viewId: '<viewId>'
,
  ...
]
```
