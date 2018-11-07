# Minio connector

This is a basic connector for [Minio](https://docs.minio.io/).

## Data provider configuration

* `type`: `"Minio"`
* `access_key`: str, required
* `secret_key`: str, required

```coffee
DATA_PROVIDERS: [
  type:       'Minio'
  access_key: '<accessKey>'
  secret_key: '<secretKey>'
]
```


## Data source configuration

* `bucketname`: str, required
* `objectname`: str, required
* `separator`: str, optional

```coffee
DATA_SOURCES: [
  bucketname: 'mybucket'
  objectname: 'myobject'
  separator:  '\t'
]
```
