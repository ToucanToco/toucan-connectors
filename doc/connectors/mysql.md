# MySQL connector

Import data from MySQL database.

## Data provider configuration

* `type`: `"MySQL"`
* `name`: str, required
* `type`: str
* `cache_ttl`: int
* `identifier`: str
* `secrets_storage_version`: str, defaults to 1
* `host`: str, required
* `port`: int
* `user`: str, required
* `password`: SecretStr
* `charset`: str, defaults to utf8mb4
* `connect_timeout`: int
* `ssl_ca`: SecretStr. The CA certificate content in PEM format to use to connect to the MySQL
  server. Equivalent of the --ssl-ca option of the MySQL client
* `ssl_cert`: SecretStr. The X509 certificate content in PEM format to use to connect to the MySQL
  server. Equivalent of the --ssl-cert option of the MySQL client
* `ssl_key`: SecretStr. The X509 certificate key content in PEM format to use to connect to the MySQL server. Equivalent of the --ssl-key option of the MySQL client
* `ssl_mode`: SSLMode. SSL Mode to use to connect to the MySQL server. Equivalent of
  the --ssl-mode option of the MySQL client. **Must be set in order to use SSL**. If
  set, must be one of `REQUIRED`, `VERIFY_CA` or `VERIFY_IDENTITY`.

```coffee
DATA_PROVIDERS: [
  type:    '<type>'
  name:    '<name>'
  cache_ttl:    '<cache_ttl>'
  identifier:    '<identifier>'
  secrets_storage_version:    '<secrets_storage_version>'
  host:    '<host>'
  port:    '<port>'
  user:    '<user>'
  password:    '<password>'
  charset:    '<charset>'
  connect_timeout:    '<connect_timeout>'
  ssl_ca:    '<ssl_ca>'
  ssl_cert:    '<ssl_cert>'
  ssl_key:    '<ssl_key>'
  ssl_mode:    '<ssl_mode>'
,
  ...
]
```


## Data source configuration

Either `query` or `table` are required, both at the same time are not supported.

* `domain`: str, required
* `name`: str, required
* `cache_ttl`: int
* `database`: str, required
* `follow_relations`: bool
* `table`: str
* `query`: str (not empty)
* `query_object`: dict
* `language`: str, defaults to sql

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  cache_ttl:    '<cache_ttl>'
  database:    '<database>'
  follow_relations:    '<follow_relations>'
  table:    '<table>'
  query:    '<query>'
  query_object:    '<query_object>'
  language:    '<language>'
,
  ...
]
```
