# Hive connector

## Data provider configuration

* `type`: `"Hive"`
* `name`: str, required
* `host`: str, required
* `port`: int, default to 10000
* `auth`: str, default to `'NONE'`, available values: `'LDAP'`, `'NOSASL'`, `'KERBEROS'`
* `configuration`: dict, a dictionary of Hive settings.
* `kerberos_service_name`: str, use with `auth: 'KERBEROS'` only
* `username`: str
* `password`: str, use with `auth: 'LDAP'` only

```coffee
DATA_PROVIDERS: [
  type:    'Hive'
  name:    '<name>'
  host:    '<host>'
  port:    '<port>'
  auth:    '<auth>'
  configuration:    '<configuration>'
  kerberos_service_name:    '<kerberos_service_name>'
  username:    '<username>'
  password:    '<password>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `database`: str, default to `'default'`
* `query`: str, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  database:    '<database>'
  query:    '<query>'
,
  ...
]
```
