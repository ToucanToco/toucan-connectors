# MicroStrategy connector

Import data from MicroStrategy using the [JSON Data API](http://bit.ly/2HCzf04) for cubes and
    reports.

## Data provider configuration

* `type`: `"MicroStrategy"`
* `name`: str, required
* `base_url`: str, required
* `username`: str, required
* `password`: str, required
* `project_id`: str, required

```coffee
DATA_PROVIDERS: [
  type:    'MicroStrategy'
  name:    '<name>'
  base_url:    '<base_url>'
  username:    '<username>'
  password:    '<password>'
  project_id:    '<project_id>'
,
  ...
]
```


## Data source configuration

`dataset` parameter lets you specify whether you want to use the `cube` or `reports` endpoints, with the specified `id`. `viewfilter` allows you to refine the data returned from a report or cube (see [usage](https://lw.microstrategy.com/msdz/MSDL/GARelease_Current/_GARelease_Archives/1010/docs/projects/RESTSDK/Content/topics/REST_API/REST_API_ViewFilter.htm)).

`limit` and `offset` parameters allow you to "paginate" the data returned. If you just want the full result, set limit to `-1`.

* `domain`: str, required
* `name`: str, required
* `dataset`: str, `cube` or `report` or `search`, required
* `id`: str
* `viewfilter`: dict
* `offset`: int, default to 0
* `limit`: int, default to 100 (set it to `-1` if you want no limit)
* `parameters` dict, optional. Allow to parameterize the viewfilter.

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  dataset:    '<dataset>'
  id:    '<id>'
  viewfilter:    '<viewfilter>'
  offset:    '<offset>'
  limit:    '<limit>'
,
  ...
]
```

### Search amongst available cubes and reports

Set `dataset` to `search`:

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:      '<name>'
  dataset:   'search'
  id: "revenue analysis"  # id is optional here, and allows to filter by pattern
]
```

### enhanced viewFilter

Microstrategy's `viewFilter` does not natively support filtering by attribute or metric name, but we provide "syntaxic sugar" for them.

**Example**: instead of writing :

```coffee
viewfilter:
  operator: "Equals"
  operands: [
    type: "form"
    attribute:
      id: "8D679D3511D3E4981000E787EC6DE8A4"
    form:
      id: "CCFBE2A5EADB4F50941FB879CCF1721C"
  ,
    type: "constant"
    dataType: "Char"
    value: "Miami"
  ]
```

you can write the equivalent:

```coffee
viewfilter:
  operator": "Equals"
  operands: [
    attribute: "Call Center@DESC"
  ,
    constant: "Miami"
  ]
```

If `attribute`, `metric` or `constant` keys are found in the viewfilter, they will be expanded (to achieve this, a first api call is done to fetch the dataset fields definition).

#### attribute

```
{
  attribute: 'Call Center'
}
```

is expanded to

```
{
  'type': 'attribute',
  'id': '8D679D3511D3E4981000E787EC6DE8A4'
}
```

#### form ("attribute@form")

```
{
  attribute: 'Call Center@DESC'
}
```

is expanded to

```
{
  'type': 'form',
  'attribute': {'id': '8D679D3511D3E4981000E787EC6DE8A4'},
  'form': {'id': 'CCFBE2A5EADB4F50941FB879CCF1721C'}
}
```

#### metric

```
{
  metric: '% Change to Profit'
}
```

is expanded to

```
{
  'type': 'metric',
  'attribute': {'id': '8D679D3511D3E4981000E787EC6DE8A4'},
}
```

#### constant

(limited to Real and Char datatypes)

```
{
  constant: 42
}
```

is expanded to

```
{
  'type': 'constant',
  'dataType': 'Real',
  'value': '42'
}
```
