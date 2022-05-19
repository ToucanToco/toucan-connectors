# Anaplan connector

## Data provider configuration

* `type`: `"Anaplan"`
* `name`: str, required
* `type`: str
* `cache_ttl`: int
* `identifier`: str
* `secrets_storage_version`: str, defaults to 1
* `username`: str, required. Your Anaplan username. This is often the e-mail associated to your Anaplan account
* `password`: SecretStr, required. The password to your Anaplan account.

```coffee
DATA_PROVIDERS: [
  type:    '<type>'
  name:    '<name>'
  cache_ttl:    '<cache_ttl>'
  identifier:    '<identifier>'
  secrets_storage_version:    '<secrets_storage_version>'
  username:    '<username>'
  password:    '<password>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `cache_ttl`: int
* `model_id`: str (not empty), required
* `view_id`: str (not empty), required
* `workspace_id`: str, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  cache_ttl:    '<cache_ttl>'
  model_id:    '<model_id>'
  view_id:    '<view_id>'
  workspace_id:    '<workspace_id>'
,
  ...
]
```
