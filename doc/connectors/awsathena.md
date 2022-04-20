# Awsathena connector

## Data provider configuration

* `type`: `"Awsathena"`
* `name`: str, required
* `cache_ttl`: int
* `identifier`: str
* `secrets_storage_version`: str, default to 1
* `s3_output_bucket`: str, required. Your S3 Output bucket (where query results are stored.)
* `aws_access_key_id`: str, required. Your AWS access key ID.
* `aws_secret_access_key`: str, required, Your AWS secret key.
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
* `query`: str (not empty), required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  cache_ttl:    '<cache_ttl>'
  database:    '<database>'
  query:    '<query>'
,
  ...
]
```
