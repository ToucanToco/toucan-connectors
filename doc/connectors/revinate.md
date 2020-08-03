# Revinate Connector

Revinate connector cf. https://porter.revinate.com/documentation

# Data Provider Configuration

* `type`: `Revinate`
* `name`: str, required
* `authentication`: see below

```coffee
DATA_PROVIDERS: [
    type: 'Revinate',
    name: '<name>',
    authentication: '<authentication>'
]
```

# Revinate Authentication


Please see the Revinate documentation to understand what form the headers must take before a request can be made to their API. This essential element of the connector must have the following form:

* `api_key`: str, required
* `api_secret`: str, required
* `username`: str, required

The `api_key`, `api_secret` and `user_name` must be obtained from Revinate before the concepteur can use the connector.

The timestamp required in the headers is handled by the connector.

# Data Source Configuration

* `domain`: str, required
* `name`: str, required
* `endpoint`: str, required
* `params`: dict, optional
* `filter`: str, [`jq` filter](https://stedolan.github.io/jq/manual/)

Please note that the `endpoint` must be a valid Revinate endpoint. Params are optional and can be set much like those in the HttpAPI connector. They must be valid Revinate parameters however.

The default jq filter is `'.'`.

```coffee
DATA_SOURCES: [
    domain: '<domain>',
    name: '<domain>',
    endpoint: '<endpoint>',
    params: '<params>',
    filter: '<filter>'
]
```