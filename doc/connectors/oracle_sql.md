# OracleSQL connector

⚠️  Using this connector requires the installation of [Oracle Instant client](http://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html) library. The easiest way to install the package is to follow the different steps presents on the Oracle Github [installation instructions](https://oracle.github.io/odpi/doc/installation.html#).
Alternatively, you can refer to the Oracle website [installation instructions](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/instant-client.html#GUID-7D65474A-8790-4E81-B535-409010791C2F) as it probably won't be available in your current server package manager.

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
