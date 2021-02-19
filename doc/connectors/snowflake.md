# Snowflake connector

Import data from Snowflake data warehouse.

## Data provider configuration

* `type`: `"Snowflake"`
* `name`: str, required
* `authentication_method`, str, the authentication mechanism that will be used against Snowflake's APIs (default: plain text)
* `user`: str, required
* `password`: str, required
* `oauth_token`: str, an OAuth token
* `oauth_args`: Dict, a dict that contains furthermore information for OIDC-based authentication
  * It should at least contains the following keys and values:
    * `token_endpoint` (required): the endpoint that will be used to refresh the given token, this value can be templated
    * `refresh_token` (required): a refresh token, this value can be templated
    * `client_id` (required): a client id, this value can be templated
    * `client_secret` (required): a client secret, this value can be templated
    * `content_type` (optional): the default content type used to refresh a token is `application/json`, setting this value will override the default one
    * `exp` (required): a timestamp that defines the expiration time of the provided `oauth_token`
* `default_warehouse`: str, name of the default warehouse to be used in a data source if no warehouse was specified in the concerned data source
* `account`: str, required
* `ocsp_response_cache_filename`: str, path to the location used to store [ocsp cache] (https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#caching-ocsp-responses)

```coffee
DATA_PROVIDERS: [
  type:    'Snowflake'
  name:    '<name>'
  user:    '<user>'
  oauth_token:   '<oauth_token>'
  oauth_args:   {
    'token_endpoint': ...
    'refresh_token': ...
  }
  password:    '<password>'
  account:    '<account>'
  ocsp_response_cache_filename:    <ocsp_response_cache_filename>
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str (not empty), required
* `database`: str
* `warehouse`: str

Be sure to create your database(s) in snowflake **before** creating the data source, if no databases are found the field `database` will be empty in the creation form.

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
  database:    '<database>'
  warehouse:    '<warehouse>'
,
  ...
]
```
