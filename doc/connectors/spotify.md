# Spotify connector

## Data provider configuration

* `type`: `"Spotify"`
* `name`: str, required
* `secrets_storage_version`: str, default to 1
* `client_id`: str, default to 
* `client_secret`: str, default to 

```coffee
DATA_PROVIDERS: [
  type:    '<type>'
  name:    '<name>'
  secrets_storage_version:    '<secrets_storage_version>'
  client_id:    '<client_id>'
  client_secret:    '<client_secret>'
,
  ...
]
```


## Data source configuration

* `domain`: str, default to https://spotify.com
* `name`: str, default to spotify
* `query`: str, default to daft punk
* `limit`: int, default to 10

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
  limit:    '<limit>'
,
  ...
]
```
