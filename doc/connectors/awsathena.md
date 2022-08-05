# Awsathena connector

## Data provider configuration

* `type`: `"Awsathena"`
* `name`: str, required
* `type`: str
* `cache_ttl`: int
* `identifier`: str
* `secrets_storage_version`: str, defaults to 1
* `s3_output_bucket`: str, required
* `aws_access_key_id`: str, required
* `aws_secret_access_key`: SecretStr
* `region_name`: str, required

```coffee
DATA_PROVIDERS: [
  type:    '<type>'
  name:    '<name>'
  cache_ttl:    '<cache_ttl>'
  identifier:    '<identifier>'
  secrets_storage_version:    '<secrets_storage_version>'
  s3_output_bucket:    '<s3_output_bucket>'
  aws_access_key_id:    '<aws_access_key_id>'
  aws_secret_access_key:    '<aws_secret_access_key>'
  region_name:    '<region_name>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `cache_ttl`: int
* `database`: str (not empty), required
* `table`: str
* `language`: str, defaults to sql
* `query`: str (not empty)
* `query_object`: dict
* `use_ctas`: bool, defaults to False

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  cache_ttl:    '<cache_ttl>'
  database:    '<database>'
  table:    '<table>'
  language:    '<language>'
  query:    '<query>'
  query_object:    '<query_object>'
  use_ctas:    '<use_ctas>'
,
  ...
]
```
