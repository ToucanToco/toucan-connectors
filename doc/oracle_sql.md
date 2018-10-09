# OracleSQL connector

⚠️ Using this connector requires the installation of [Oracle Instant client](http://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html) library. Please refer to Oracle [installation instructions](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/instant-client.html#GUID-7D65474A-8790-4E81-B535-409010791C2F) as it probably won't be available in your server current package manager.

## Data provider configuration

* `type`: `"OracleSQL"`
* `name`: str, required
* `dsn`: str following the [DSN pattern](https://en.wikipedia.org/wiki/Data_source_name), required. The `host`, `port` and `service name` part of the dsn are required. For example: `localhost:80/service` 
* `user`: str
* `password`: str
* `encoding`: str

```coffee
DATA_PROVIDERS: [
  type:    'OracleSQL'
  name:    '<name>'
  dsn:    <dsn>
  user:    '<user>'
  password:    '<password>'
  encoding:    '<encoding>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
,
  ...
]
```
