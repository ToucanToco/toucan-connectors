# Elasticsearch connector

## Data provider configuration

* `type`: `"elasticsearch"`
* `name`: str, required
* `hosts`: list of Host, required


### Host configuration
* `url`: str, required
* `port`: int
* `username`: str
* `password`: str
* `headers`: dict


```coffee
DATA_PROVIDERS: [
  type:    'elasticsearch'
  name:    '<name>'
  hosts:    '<hosts>'
,
  ...
]
```


## Data source configuration

* `domain`: str, required
* `name`: str, required
* `search_method`: `search` or `msearch`, required
* `body`: dict for `search` or list for `msearch`, required
* `index`: str (required for `search`)
* `parameters`: dict

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  search_method:    '<search_method>'
  body:    '<body>'
  index:    '<index>'
  parameters:    '<parameters>'
,
  ...
]
```

## Elasticsearch Search
Data will correspond to the field `_source` of the API response.
See API documentation for :
* _search : https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
* _msearch : https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html
